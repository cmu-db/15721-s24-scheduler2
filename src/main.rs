pub mod api;
mod composable_database;
mod dispatcher;
pub mod integration_test;
pub mod mock_executor;
pub mod parser;
mod query_graph;
mod query_table;
mod scheduler;
mod task_queue;

use std::path::PathBuf;
use clap::{App, Arg, SubCommand};
use config::{Config, ConfigError, File, FileFormat};
use datafusion::error::DataFusionError;
use serde::Deserialize;
use tonic::transport::Server;

use crate::api::{composable_database::scheduler_api_server::SchedulerApiServer, SchedulerService};
use crate::integration_test::{read_config, start_executor_client, start_scheduler_server};
use std::io::{self, Write};
use datafusion::prelude::CsvReadOptions;
use walkdir::WalkDir;
use crate::mock_executor::DatafusionExecutor;

pub enum SchedulerError {
    Error(String),
    DfError(DataFusionError),
}

#[derive(Debug, Deserialize)]
struct Executor {
    #[serde(default)]
    id: u64,
    numa_node: u16,
    ip_addr: String,
    port: u16,
}

#[derive(Debug, Deserialize)]
struct Executors {
    executors: Vec<Executor>,
}

impl Executors {
    fn new() -> Self {
        Executors {
            executors: Vec::new(),
        }
    }

    fn from_file() -> Result<Self, ConfigError> {
        let executors: Result<Executors, _> = Config::builder()
            .add_source(File::new(EXECUTOR_CONFIG, FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize();
        executors
    }
}

const EXECUTOR_CONFIG: &str = "executors.toml";

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let crate_root = env!("CARGO_MANIFEST_DIR");
//     println!("Path to crate's root: {}", crate_root);
//     let addr = "[::1]:50051".parse()?;
//     let scheduler_service = SchedulerService::default();
//     Server::builder()
//         .add_service(SchedulerApiServer::new(scheduler_service))
//         .serve(addr)
//         .await?;
//     Ok(())
// }
//

fn load_context() {}






async fn initialize_executor() -> DatafusionExecutor {
    let executor = DatafusionExecutor::new();

    for entry in WalkDir::new("./test_files") {
        let entry = entry.unwrap();
        if entry.file_type().is_file() && entry.path().extension().map_or(false, |e| e == "csv") {
            let file_path = entry.path().to_str().unwrap();

            // Extract the table name from the file name without the extension
            let table_name = entry.path().file_stem().unwrap().to_str().unwrap();

            let options = CsvReadOptions::new();

            // Register the CSV file as a table
            let result = executor.register_csv(table_name, file_path, options).await;
            assert!(
                result.is_ok(),
                "Failed to register CSV file: {:?}",
                file_path
            );
        }
    }
    executor
}

/**

    Config:
    config file parser, can start mock executors and mock gRPC server

    "front-end"
    Step 2: enter either interactive mode / file mode
    interactive mode: run SQL query in command line, plan and send to scheduler as gRPC
    file mode: .slt files containing SQL statements, plan and send to scheduler as gRPC

    "mock executor":
    can do handshakes, and execute executionplans but HOW TO PASS RESULT and how to verify result after the scheduler finishes a query

*/
#[tokio::main]
async fn main() {
    let matches = App::new("DBMS Test CLI")
        .version("0.1")
        .about("Command line tool for DBMS end-to-end testing")
        .subcommand(SubCommand::with_name("interactive")
            .about("Enter interactive SQL mode"))
        .subcommand(SubCommand::with_name("file")
            .about("Execute SQL logic tests from a file")
            .arg(Arg::with_name("FILE")
                .help("Sets the input file to use")
                .required(true)
                .index(1)))
        .get_matches();

    match matches.subcommand() {
        Some(("interactive", _)) => {
            interactive_mode();
        }
        Some(("file", file_matches)) => {
            if let Some(file_path) = file_matches.value_of("FILE") {
                file_mode(PathBuf::from(file_path));
            } else {
                eprintln!("File path not provided.");
            }
        }
        None => {
            panic!("Usage: ./main interactive or ./main file <path-to-sqllogictest>");
        }

        _ => {panic!("Usage: ./main interactive or ./main file <path-to-sqllogictest>");}
    }
}

fn interactive_mode() {
    println!("Entering interactive mode. Type your SQL queries or 'exit' to quit:");


    let config = read_config();

    // Start the scheduler
    let scheduler_addr = format!("{}:{}", config.scheduler.id_addr, config.scheduler.port);
    let scheduler_addr_for_server = scheduler_addr.clone();
    tokio::spawn(async move {
        start_scheduler_server(&scheduler_addr_for_server).await;
    });

    // Start executor clients
    for executor in config.executors {
        // Clone the scheduler_addr for each executor client
        let scheduler_addr_for_client = scheduler_addr.clone();
        tokio::spawn(async move {
            start_executor_client(executor, &scheduler_addr_for_client).await;
        });
    }

    // Implement interactive SQL input and execution

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

        // Handle the SQL input here...
        println!("You entered: {}", trimmed_input);




    }

}

fn file_mode(file_path: PathBuf) {
    println!("Executing tests from file: {:?}", file_path);
    // Implement test execution logic
    // On failure, log the error and exit
}

