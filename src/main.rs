mod executor_client;
mod frontend;
pub mod integration_test;
pub mod intermediate_results;
mod mock_executor;
mod mock_optimizer;
pub mod parser;
pub mod project_config;
mod query_graph;
mod query_table;
mod server;
mod task;
mod task_queue;

use crate::executor_client::ExecutorClient;
use crate::integration_test::IntegrationTest;
use crate::parser::ExecutionPlanParser;
use crate::server::composable_database::QueryStatus::{Done, InProgress};
use clap::{App, Arg, SubCommand};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use futures::TryFutureExt;
use prost::Message;
use std::io::{self, Write};
use std::time::Duration;

pub enum SchedulerError {
    Error(String),
    DfError(DataFusionError),
}

#[tokio::main]
async fn main() {
    let matches = App::new("Scheduler Test CLI")
        .version("0.1")
        .about("Command line tool for DBMS end-to-end testing")
        .subcommand(SubCommand::with_name("interactive").about("Enter interactive SQL mode"))
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
const POLL_INTERVAL: Duration = Duration::from_millis(100);

// creates server, executors, and the frontend
pub async fn start_system() -> IntegrationTest {
    const STARTUP_TIME: Duration = Duration::from_millis(2000);

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;

    tester.run_server().await;
    tokio::time::sleep(STARTUP_TIME).await;

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

pub async fn file_mode(file_paths: Vec<&str>, verify_correctness: bool) {
    let tester = start_system().await;
    let parser = ExecutionPlanParser::new(CATALOG_PATH).await;

    let mut reference_solutions = Vec::new();
    for file_path in &file_paths {
        // Generate reference solution first
        let reference_solution = tester.generate_reference_results(*file_path).await;
        reference_solutions.extend(reference_solution);
    }

    let mut query_ids = Vec::new();

    // for each file selected...
    for file_path in file_paths {
        println!("Executing tests from file: {:?}", file_path);

        // Read SQL statements from file
        let sql_statements = parser
            .read_sql_from_file(&file_path)
            .await
            .unwrap_or_else(|err| {
                panic!("Unable to parse file {}: {:?}", file_path, err);
            });

        println!(
            "Generating reference result sets from file: {:?}",
            file_path
        );

        // Batch submit all queries
        for sql in sql_statements {
            match tester.frontend.lock().await.submit_job(&sql).await {
                Ok(query_id) => {
                    println!("Submitted query id: {}, query: {}", query_id, sql);
                    query_ids.push(query_id);
                }
                Err(e) => {
                    panic!("Error in submitting query: {}: {}", sql, e);
                }
            }
        }
    }

    assert_eq!(query_ids.len(), reference_solutions.len());

    // Wait until all jobs completed
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        if tester.frontend.lock().await.get_num_running_jobs().await == 0 {
            break;
        }
    }

    // Collect and print all job results
    let jobs_map = tester.frontend.lock().await.get_all_jobs();
    for (job_id, job_info) in jobs_map.iter() {
        println!("Query ID: {}, Info: {}", job_id, job_info);
    }

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
}

#[cfg(test)]
mod tests {
    use crate::{file_mode, generate_reference_results};

    #[tokio::test]
    async fn test_generate_reference_results() {
        let test_sql_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql", "/1.sql");
        let results = generate_reference_results(&test_sql_path).await;
    }

    #[tokio::test]
    async fn test_file_mode() {
        let files_to_run = vec![
            "./test_sql/1.sql",
            "./test_sql/2.sql",
            "./test_sql/3.sql",
            "./test_sql/4.sql",
            "./test_sql/5.sql",
            "./test_sql/6.sql",
            "./test_sql/7.sql",
            "./test_sql/8.sql",
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

        file_mode(files_to_run, true).await;
    }
}
