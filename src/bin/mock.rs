//! # Scheduler Test Command Line Interface
//!
//! This module provides a command line interface for performing end-to-end integration testing.
//! The interface facilitates testing the system's scheduler, frontend, and
//! executor components through interactive user inputs, automated benchmarks, and execution from SQL files.
//!
//! ## Testing Modes
//!
//! - **Interactive Mode**: Allows users to manually enter SQL queries interactively. This mode is useful for
//!   ad-hoc testing and debugging, where users can input queries and immediately see results and system behavior.
//!
//! - **File Mode**: Executes a series of SQL commands from a specified file. This mode is intended for automated
//!   testing or regression tests.
//!
//! - **Benchmark Mode**: Runs a predefined set of TPC-H benchmark queries to measure the performance and efficiency
//!   of the system. This mode is crucial for performance testing and helps in understanding the scalability and
//!   throughput of the system under heavy loads.
//!
//! ## Usage
//!
//! The command line tool can be invoked with different subcommands corresponding to each testing mode:
//!
//! ```bash
//! cargo run -- interactive                # Interactive mode
//! cargo run -- file <path-to-sql-file>    # File mode with path to the SQL logic test file
//! cargo run -- benchmark                  # Benchmark mode using TPC-H queries
//! ```
//!
//! ## Architecture
//!
//! This CLI interacts with the system's components as follows:
//!
//! - **Scheduler**: Responsible for managing task execution across distributed executors.
//! - **Frontend**: Handles SQL parsing, query planning, and submission to the scheduler.
//! - **Executors**: Perform the actual execution of queries as directed by the scheduler.
//!
//! Each mode initializes these components and manages their interactions through asynchronous tasks
//!
//! ## Configuration
//!
//! - **Catalog and Config Paths**: Paths for the data catalog and system configurations are set via constants
//!   and can be adjusted according to the deployment environment. The number and IP addresses of
//!   executors can be configured in executors.toml.
//!
//! - **Polling Interval**: The frequency of status checks in ongoing tasks is configurable
//!

use scheduler2::composable_database::QueryStatus::{Done, InProgress};
use scheduler2::frontend::JobInfo;
use scheduler2::integration_test::IntegrationTest;
use scheduler2::parser::ExecutionPlanParser;
use scheduler2::profiling;
// use scheduler2::SchedulerError;
use clap::{App, Arg, SubCommand};
use datafusion::error::DataFusionError;
// use futures::TryFutureExt;
// use prost::Message;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};
// use chrono::Utc;
// use tokio::io::AsyncWriteExt;
use scheduler2::composable_database::ScheduleQueryArgs;
use scheduler2::mock_executor::MockExecutor;
use tokio::time::Instant;
use tonic::Request;

#[tokio::main]
async fn main() {
    let matches = App::new("Scheduler Test CLI")
        .version("0.1")
        .about("Command line tool for DBMS end-to-end testing")
        .subcommand(SubCommand::with_name("interactive").about("Enter interactive SQL mode"))
        .subcommand(SubCommand::with_name("benchmark").about("Enter TPC-H benchmark mode"))
        .subcommand(
            SubCommand::with_name("baseline")
                .about("Get baseline performance for each TPCH query on the machine"),
        )
        .subcommand(
            SubCommand::with_name("file")
                .about("Execute SQL logic tests from a file")
                .arg(
                    Arg::with_name("FILE")
                        .help("Sets the input file to use")
                        .required(true)
                        .index(1),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("interactive", _)) => {
            interactive_mode().await;
        }
        Some(("file", file_matches)) => {
            if let Some(file_path) = file_matches.value_of("FILE") {
                file_mode(vec![file_path], false).await;
            } else {
                eprintln!("File path not provided.");
            }
        }

        Some(("benchmark", _)) => {
            benchmark_mode().await;
        }

        Some(("baseline", _)) => {
            baseline_mode().await;
        }

        None => {
            panic!("Usage: cargo run interactive or cargo run file <path-to-sqllogictest>");
        }

        _ => {
            panic!("Usage: cargo run interactive or cargo run file <path-to-sqllogictest>");
        }
    }
}

const CONFIG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/executors.toml");
const CATALOG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");
const LOG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/executor_logs");
const POLL_INTERVAL: Duration = Duration::from_millis(100);

// creates server, executors, and the frontend
pub async fn start_system() -> IntegrationTest {
    const STARTUP_TIME: Duration = Duration::from_millis(2000);

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;

    tester.run_frontend().await;
    tokio::time::sleep(STARTUP_TIME).await;

    tester.run_client().await;
    tokio::time::sleep(STARTUP_TIME).await;

    tester
}

// submits a sql query and blocks until the result is received
pub async fn run_single_query(
    tester: &IntegrationTest,
    query: &str,
) -> Result<(), DataFusionError> {
    let query_id = tester.frontend.lock().await.submit_job(query).await?;
    loop {
        let mut frontend_lock = tester.frontend.lock().await;
        let job_info = frontend_lock
            .check_job_status(query_id)
            .await
            .unwrap_or_else(|| {
                panic!("submitted query id {} but not found on scheduler", query_id)
            });

        if job_info.status == InProgress {
            drop(frontend_lock);
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }

        println!("{}", job_info);
        drop(frontend_lock);
        return Ok(());
    }
    unreachable!();
}

async fn interactive_mode() {
    println!("Entering interactive mode. Type your SQL queries or 'exit' to quit:");

    let tester = start_system().await;

    let mut input = String::new();
    loop {
        print!("sql> ");
        io::stdout().flush().unwrap(); // flush the prompt
        input.clear();
        io::stdin().read_line(&mut input).unwrap();

        let trimmed_input = input.trim();

        // exit the loop if the user types 'exit'
        if trimmed_input.eq_ignore_ascii_case("exit") {
            break;
        }

        run_single_query(&tester, trimmed_input)
            .await
            .unwrap_or_else(|err| {
                eprintln!("Error running query {}: {}", trimmed_input, err);
            });
    }
}

pub async fn file_mode(file_paths: Vec<&str>, verify_correctness: bool) -> HashMap<u64, JobInfo> {
    let tester = start_system().await;
    let parser = ExecutionPlanParser::new(CATALOG_PATH).await;

    println!("Generating reference solutions...");
    let mut reference_solutions = Vec::new();
    let mut request_pairs: Vec<(String, Request<ScheduleQueryArgs>)> = Vec::new();

    let start = SystemTime::now();
    for file_path in &file_paths {
        // Generate reference solution first
        let reference_solution = tester.generate_reference_results(*file_path).await;
        reference_solutions.extend(reference_solution);

        // Read SQL statements from file
        let sql_statements = parser
            .read_sql_from_file(&file_path)
            .await
            .unwrap_or_else(|err| {
                panic!("Unable to parse file {}: {:?}", file_path, err);
            });

        // Prepare requests for all sql queries
        for sql in sql_statements {
            let request_pair = tester
                .frontend
                .lock()
                .await
                .sql_to_job_request(&sql)
                .await
                .expect("fail to create request for sql");
            request_pairs.push(request_pair);
        }
    }
    let mut frontend = tester.frontend.lock().await;

    let mut query_ids = Vec::new();
    let mut last_time = Instant::now(); // Initialize the timer using tokio::time

    for request_pair in request_pairs {
        let sql_query = request_pair.0.clone();

        // Record the time just before the request is submitted
        let start_time = Instant::now();

        match frontend.submit_request(request_pair).await {
            Ok(query_id) => {
                let elapsed = start_time.elapsed(); // Calculate time elapsed since the request start
                println!(
                    "Submitted query id: {}, query: {}, took {:?} to submit",
                    query_id, sql_query, elapsed
                );
                query_ids.push(query_id);
            }
            Err(e) => {
                // Log error without stopping the execution
                eprintln!("Error in submitting query: {}: {}", sql_query, e);
            }
        }

        last_time = Instant::now(); // Reset the timer for the next iteration
    }

    println!("Total operations time: {:?}", last_time.elapsed());

    drop(frontend);

    assert_eq!(query_ids.len(), reference_solutions.len());

    // Wait until all jobs completed
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        if tester.frontend.lock().await.get_num_running_jobs().await == 0 {
            break;
        }
    }

    let time = SystemTime::now().duration_since(start).unwrap();
    println!("Execution time: {:?}ms", time.as_millis());

    // Collect and print all job results
    let jobs_map = tester.frontend.lock().await.get_all_jobs();
    // for (job_id, job_info) in jobs_map.iter() {
    //     println!("Query ID: {}, Info: {}", job_id, job_info);
    // }

    if verify_correctness {
        for (i, query_id) in query_ids.iter().enumerate() {
            let job_info = jobs_map
                .get(&query_id)
                .expect("query not in job map")
                .clone();
            assert_eq!(job_info.status, Done);
            let query_results = job_info.result.expect("empty job info");
            let are_equal = tester
                .results_eq(&query_results, &reference_solutions[i])
                .await
                .expect("fail to compare results");
            if !are_equal {
                panic!("file_mode: comparison failed on query id {}", query_id);
            }
        }
    }

    jobs_map.clone()
}

const TPCH_FILES: &[&str] = &[
    "./test_sql/1.sql",
    "./test_sql/2.sql",
    "./test_sql/3.sql",
    "./test_sql/4.sql",
    "./test_sql/5.sql",
    "./test_sql/6.sql",
    "./test_sql/7.sql",
    // TODO: query 8 causes stack overlflow in parser.rs
    // "./test_sql/8.sql",
    "./test_sql/9.sql",
    "./test_sql/10.sql",
    "./test_sql/11.sql",
    "./test_sql/12.sql",
    "./test_sql/13.sql",
    "./test_sql/14.sql",
    "./test_sql/15.sql",
    "./test_sql/16.sql",
    "./test_sql/17.sql",
    "./test_sql/18.sql",
    "./test_sql/19.sql",
    "./test_sql/20.sql",
    "./test_sql/21.sql",
    "./test_sql/22.sql",
];

pub async fn baseline_mode() {
    // Initialize executor and parser with the catalog path
    let executor = MockExecutor::new(CATALOG_PATH).await;
    let parser = ExecutionPlanParser::new(CATALOG_PATH).await;

    // Create a CSV file called baseline_execution_times.csv
    let file = File::create("baseline_execution_times.csv").expect("failed to create file");
    let mut writer = BufWriter::new(file);
    writeln!(writer, "tpch_query_id,execution_time_ms").expect("failed to write headers");

    let mut tpch_query_id = 1;
    for file_path in TPCH_FILES {
        // Parse the SQL execution plan from the file
        let sql_plan = parser
            .get_execution_plan_from_file(file_path)
            .await
            .expect("fail to get physical plan");

        assert_eq!(1, sql_plan.len());

        // Record the start time
        let start = Instant::now();

        // Execute the query
        executor
            .execute(sql_plan[0].clone())
            .await
            .expect("fail to execute query");

        // Record the end time and compute the difference
        let duration = Instant::now().duration_since(start);
        let duration_ms = duration.as_millis(); // converting duration to milliseconds

        // Write the results to the CSV file
        writeln!(writer, "{},{}", tpch_query_id, duration_ms).expect("failed to write data");

        tpch_query_id += 1;
    }

    // Ensure all data is flushed to the file
    writer.flush().expect("failed to flush data");
}

pub async fn benchmark_mode() {
    const JOB_SUMMARY_OUTPUT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/job_summary.json");

    let job_map = file_mode(TPCH_FILES.to_vec(), true).await;

    profiling::write_jobs_to_json(
        job_map.values().cloned().collect(),
        Path::new(JOB_SUMMARY_OUTPUT_PATH),
    )
    .await
    .unwrap_or_else(|err| {
        panic!("Fail to write job summary: {}", err);
    });
}

#[cfg(test)]
mod tests {

    use crate::file_mode;
    use crate::TPCH_FILES;

    #[tokio::test]
    async fn test_file_mode() {
        file_mode(TPCH_FILES.to_vec(), true).await;
    }
}
