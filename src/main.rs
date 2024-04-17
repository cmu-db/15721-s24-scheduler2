mod executor;
pub mod integration_test;
pub mod intermediate_results;
mod mock_frontend;
pub mod parser;
pub mod project_config;
mod query_graph;
mod query_table;
mod server;
mod task;
mod task_queue;

use crate::integration_test::IntegrationTest;
use clap::{App, Arg, SubCommand};
use datafusion::error::DataFusionError;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;
use crate::parser::ExecutionPlanParser;

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

async fn interactive_mode() {
    println!("Entering interactive mode. Type your SQL queries or 'exit' to quit:");

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;
    tester.run_server().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let frontend = tester.run_frontend().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    tester.run_client().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

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

        match frontend.run_sql(trimmed_input).await {
            Ok(res) => {
                println!("Result: {:?}", res);
            }

            Err(e) => {
                println!("Error in running query: {}", e);
            }
        }
    }
}

async fn file_mode(file_path: String) {
    println!("Executing tests from file: {:?}", file_path);

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;
    tester.run_server().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let frontend = tester.run_frontend().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    tester.run_client().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let parser = ExecutionPlanParser::new(CATALOG_PATH).await;


    

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



        match frontend.run_sql(trimmed_input).await {
            Ok(res) => {
                println!("Result: {:?}", res);
            }

            Err(e) => {
                println!("Error in running query: {}", e);
            }
        }
    }
}
