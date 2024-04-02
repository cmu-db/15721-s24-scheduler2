pub mod api;
mod composable_database;
mod dispatcher;
// pub mod integration_test;
pub mod intermediate_results;
pub mod mock_executor;
pub mod parser;
pub mod project_config;
mod query_graph;
mod query_table;
mod task_queue;

pub mod integration_test;

mod mock_frontend;
mod reference_executor;
mod task;

use crate::integration_test::IntegrationTest;
use clap::{App, Arg, SubCommand};
use datafusion::error::DataFusionError;
use project_config::Config;
use serde::Deserialize;
use std::path::PathBuf;
use tonic::transport::Server;

use crate::api::{composable_database::scheduler_api_server::SchedulerApiServer, SchedulerService};
// use crate::integration_test::{read_config, start_scheduler_server};
use crate::mock_executor::DatafusionExecutor;
use crate::parser::DFColumnType;
use datafusion::execution::context::DataFilePaths;
use datafusion::prelude::CsvReadOptions;
use sqllogictest::Record;
use std::io::{self, Write};
use std::time::Duration;
use walkdir::WalkDir;

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

    loop{}

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

const CONFIG_PATH: &str = "executors.toml";
const CATALOG_PATH: &str = "./test_files/";

async fn interactive_mode() {
    println!("Entering interactive mode. Type your SQL queries or 'exit' to quit:");

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;
    tester.run_server().await;
    tokio::time::sleep(Duration::from_millis(2000));
    tester.run_client().await;
    tokio::time::sleep(Duration::from_millis(2000));
    let frontend = tester.run_frontend().await;
    tokio::time::sleep(Duration::from_millis(2000));

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

        println!("You entered: {}", trimmed_input);

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

async fn file_mode(file_path: PathBuf) {
    println!("Executing tests from file: {:?}", file_path);

    let tester = IntegrationTest::new(CATALOG_PATH.to_string(), CONFIG_PATH.to_string()).await;
    tester.run_server().await;
    tester.run_client().await;
    let frontend = tester.run_frontend().await;

    let sql_statements: Vec<Record<DFColumnType>> =
        sqllogictest::parse_file(file_path).expect("failed to parse file");
}
