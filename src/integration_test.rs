/*

Steps for integration testing:
1. Start the scheduler gRPC
2. Start executors according to the config file


Task 1: figure out how to import mock executor
Task 2: write unit tests for mock executor
Task 3: make integration tests compile




*/

use crate::api::composable_database::scheduler_api_client::SchedulerApiClient;
use crate::api::composable_database::scheduler_api_server::SchedulerApiServer;
use crate::api::composable_database::TaskId;
use crate::api::composable_database::{NotifyTaskStateArgs, ScheduleQueryArgs};
use crate::api::SchedulerService;
use crate::mock_executor::DatafusionExecutor;
use crate::parser::{get_execution_plan_from_file, list_all_slt_files};
use crate::scheduler::Scheduler;
use lazy_static::lazy_static;
use serde::Deserialize;
use std::env;
use std::fs;
use tonic::transport::{Channel, Server};

lazy_static! {
    static ref HANDSHAKE_QUERY_ID: u64 = -1i64 as u64;

    static ref HANDSHAKE_TASK_ID: u64 = -1i64 as u64;

    static ref HANDSHAKE_TASK: TaskId = TaskId {
        query_id: *HANDSHAKE_QUERY_ID,
        task_id: *HANDSHAKE_TASK_ID,
    };

    static ref
}

// Format definitions for the config file
#[derive(Deserialize)]
struct Config {
    scheduler: Vec<SchedulerConfig>,
    executors: Vec<ExecutorConfig>,
}

#[derive(Deserialize)]
struct SchedulerConfig {
    ip_addr: String,
    port: u16,
}

#[derive(Deserialize)]
struct ExecutorConfig {
    id: u32,
    ip_addr: String,
    port: u16,
    numa_node: u32,
}

fn read_config() -> Config {
    let config_str = fs::read_to_string("config.toml").expect("Failed to read config file");
    toml::from_str(&config_str).expect("Failed to parse config file")
}

// Starts the scheduler gRPC service
async fn start_scheduler_server(addr: &str) {
    let addr = addr.parse().expect("Invalid address");

    println!("Scheduler listening on {}", addr);

    let scheduler_service = SchedulerService::default();
    Server::builder()
        .add_service(SchedulerApiServer::new(scheduler_service))
        .serve(addr)
        .await
        .expect("unable to start scheduler gRPC server");
}



// Starts the executor gRPC service
async fn start_executor_client(executor: ExecutorConfig, scheduler_addr: &str) {
    println!(
        "Executor {} connecting to scheduler at {}",
        executor.id, scheduler_addr
    );

    // Create a connection to the scheduler
    let channel = Channel::from_shared(scheduler_addr.to_string())
        .expect("Invalid scheduler address")
        .connect()
        .await
        .expect("Failed to connect to scheduler");

    // Create a client using the channel
    let mut client = SchedulerApiClient::new(channel);

    let executor = DatafusionExecutor::new();

    // Send initial request with handshake task ID

    let handshake_req = NotifyTaskStateArgs {
        task: Some(TaskId {
            query_id: *HANDSHAKE_QUERY_ID,
            task_id: *HANDSHAKE_TASK_ID,
        }),
        success: true,
        result: Vec::new(),
    };

    let mut task_id = TaskId {
        query_id: *HANDSHAKE_QUERY_ID,
        task_id: *HANDSHAKE_TASK_ID,
    };

    loop {
        let request =
            if task_id.query_id == *HANDSHAKE_QUERY_ID && task_id.task_id == *HANDSHAKE_TASK_ID {
                tonic::Request::new(handshake_req.clone())
            } else {
                tonic::Request::new(NotifyTaskStateArgs {
                    task: Some(task_id.clone()),
                    success: true,
                    result: Vec::new(),
                })
            };

        match client.notify_task_state(request).await {
            Ok(response) => {
                let response_inner = response.into_inner();
                if response_inner.has_new_task {
                    task_id = response_inner.task.unwrap_or_default();

                    // TODO: execute the physical plan

                    // let new_result
                } else {
                    // No new task available
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to send task state: {}", e);
                break;
            }
        }
    }
}

// TODO: add a function to run a sql query on a single executor

// TODO: research function to compare equality of two results (apache arrow?)

fn generate_refsol() {}

// Make this into a command line app
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = read_config();

    let args: Vec<String> = env::args().collect();
    // Check if at least one argument was provided (excluding the program name)
    if args.len() != 1 {
        panic!("Usage: pass in the .slt file you want to test");
    }

    let file_path = &args[1];
    eprintln!("Start running file {}", file_path);

    // Start the scheduler server

    let scheduler_addr = format!(
        "{}:{}",
        config.scheduler[0].ip_addr, config.scheduler[0].port
    );

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

    // start an reference executor instance to verify correctness
    let reference_executor = DatafusionExecutor::new();

    // get all the execution plans and pre-compute all the reference results
    let execution_plans = get_execution_plan_from_file(file_path)
        .await
        .expect("Failed to get execution plans");
    let mut results = Vec::new();
    for plan in execution_plans {
        match reference_executor.execute_plan(plan).await {
            Ok(dataframe) => {
                results.push(dataframe);
            }
            Err(e) => eprintln!("Failed to execute plan: {}", e),
        }
    }

    // TODO: make this a command line program where it runs a file, verify files by comparing recordbatches

    let test_files = list_all_slt_files("./test_files");

    for file_path in test_files {
        let file_path_str = file_path
            .to_str()
            .expect("Failed to convert path to string");

        eprintln!("Processing test file: {}", file_path_str);

        match get_execution_plan_from_file(file_path_str).await {
            Ok(plans) => {}
            Err(e) => {
                eprintln!(
                    "Failed to get execution plans from file {}: {}",
                    file_path_str, e
                );
                panic!("Test failed due to error with file: {}", file_path_str);
            }
        }
    }

    Ok(())
}
