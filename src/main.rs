mod executor;
pub mod integration_test;
pub mod intermediate_results;
mod mock_frontend;
mod mock_optimizer;
pub mod parser;
pub mod project_config;
mod query_graph;
mod query_table;
mod server;
mod task;
mod task_queue;

use crate::executor::Executor;
use crate::integration_test::IntegrationTest;
use crate::parser::ExecutionPlanParser;
use clap::{App, Arg, SubCommand};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;
use prost::Message;
use tokio::time::Interval;
use crate::server::composable_database::QueryStatus::{Done, InProgress};

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

    interactive_mode().await;

    loop {}

    // match matches.subcommand() {
    //     Some(("interactive", _)) => {
    //         interactive_mode();
    //     }
    //     Some(("file", file_matches)) => {
    //         if let Some(file_path) = file_matches.value_of("FILE") {
    //             file_mode(PathBuf::from(file_path));
    //         } else {
    //             eprintln!("File path not provided.");
    //         }
    //     }
    //     None => {
    //         panic!("Usage: cargo run interactive or cargo run file <path-to-sqllogictest>");
    //     }
    //
    //     _ => {
    //         panic!("Usage: cargo run interactive or cargo run file <path-to-sqllogictest>");
    //     }
    // }
}

const CONFIG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/executors.toml");
const CATALOG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");
const POLL_INTERVAL: Duration = Duration::from_millis(100);

// creates server, executors, and the frontend
async fn start_system() -> IntegrationTest {
    const startup_time: Duration = Duration::from_millis(2000);

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;

    tester.run_server().await;
    tokio::time::sleep(startup_time).await;

    tester.run_frontend().await;
    tokio::time::sleep(startup_time).await;

    tester.run_client().await;
    tokio::time::sleep(startup_time).await;

    tester
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
        
        match tester.frontend.lock().await.submit_job(trimmed_input).await {
            Ok(query_id) => {
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
                    break;
                }
            }

            Err(e) => {
                eprintln!("main: fail to run sql {}: {}", trimmed_input, e);
                continue;
            }
        }
    }
}

async fn generate_reference_results(file_path: &str) -> Vec<RecordBatch> {
    let parser = ExecutionPlanParser::new(CATALOG_PATH).await;
    let reference_executor = Executor::new(CATALOG_PATH, -1).await;
    let sql_statements = parser
        .read_sql_from_file(&file_path)
        .await
        .unwrap_or_else(|err| {
            panic!("Unable to parse file {}: {:?}", file_path, err);
        });

    // Caches the correct results for each sql query
    let mut results: Vec<RecordBatch> = Vec::new();

    // Execute each SQL statement
    for sql in sql_statements {
        let execution_result = reference_executor.execute_sql(&sql).await;

        // Check for errors in execution
        let record_batches = execution_result.unwrap_or_else(|err| {
            panic!("Error executing SQL '{}': {:?}", sql, err);
        });

        // Ensure there's exactly one RecordBatch
        if record_batches.len() == 1 {
            results.push(record_batches[0].clone());
        } else {
            panic!(
                "The SQL statement '{}' should only contain one RecordBatch, found {}",
                sql,
                record_batches.len()
            );
        }
    }

    results
}

async fn file_mode(file_path: String) {
    println!("Executing tests from file: {:?}", file_path);

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;
    tester.run_server().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    tester.run_frontend().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    tester.run_client().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let parser = ExecutionPlanParser::new(CATALOG_PATH).await;

    println!(
        "Generating reference result sets from file: {:?}",
        file_path
    );
    let reference_result_sets = generate_reference_results(&file_path).await;

    let sql_statements = parser
        .read_sql_from_file(&file_path)
        .await
        .unwrap_or_else(|err| {
            panic!("Unable to parse file {}: {:?}", file_path, err);
        });
    //
    // for sql in sql_statements {
    //     println!("Running query: {}", sql);
    //     match frontend.run_sql(&sql).await {
    //         Ok(res) => {
    //             println!("Result: {:?}", res);
    //         }
    //
    //         Err(e) => {
    //             println!("Error in running query: {}", e);
    //         }
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use crate::generate_reference_results;

    #[tokio::test]
    async fn test_generate_reference_results() {
        let test_sql_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql", "/1.sql");
        let results = generate_reference_results(&test_sql_path).await;

        // 1 since each TPC-H query only contains 1 result set
        assert_eq!(1, results.len());
    }
}
