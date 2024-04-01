use crate::api::composable_database::scheduler_api_client::SchedulerApiClient;
use crate::api::composable_database::scheduler_api_server::SchedulerApiServer;
use crate::api::composable_database::{NotifyTaskStateArgs, ScheduleQueryArgs};
use crate::api::composable_database::{NotifyTaskStateRet, TaskId};
use crate::api::SchedulerService;
use crate::mock_executor::DatafusionExecutor;
use crate::parser::{deserialize_physical_plan, get_execution_plan_from_file, list_all_slt_files};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::CsvReadOptions;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use tonic::transport::{Channel, Server};
use walkdir::WalkDir;
use clap::{App, Arg, ArgMatches, SubCommand};
use std::path::PathBuf;

/**
This gRPC facilitates communication between executors and the scheduler:
rpc NotifyTaskState(NotifyTaskStateArgs) returns (NotifyTaskStateRet);

Executor to scheduler message:
message NotifyTaskStateArgs {
    TaskID task = 1; // Task identifier
    bool success = 2; // Indicates if the task was executed successfully
    bytes result = 3; // Result data as bytes
}
- Notifies the scheduler of the task's execution status using the associated TaskID and transmits the result data.

Scheduler to executor message:
message NotifyTaskStateRet {
    bool has_new_task = 1; // Indicates the presence of a new task
    TaskID task = 2; // New task identifier
    bytes physical_plan = 3; // Task execution plan
}
- Enables the scheduler to assign new tasks to the executor.

Establishing connection:
During integration tests, executors utilize NotifyTaskStateArgs to establish initial communication with the scheduler. The initial message contains the HANDSHAKE_TASK_ID. Upon receipt, the scheduler begins assigning tasks using NotifyTaskStateRet messages.
 */

lazy_static! {
    static ref HANDSHAKE_QUERY_ID: u64 = -1i64 as u64;
    static ref HANDSHAKE_TASK_ID: u64 = -1i64 as u64;
    static ref HANDSHAKE_TASK: TaskId = TaskId {
        query_id: *HANDSHAKE_QUERY_ID,
        task_id: *HANDSHAKE_TASK_ID,
    };
}

// Checks if the message is a handshake message
fn is_handshake_message(task: &TaskId) -> bool {
    return task.query_id == *HANDSHAKE_QUERY_ID && task.task_id == *HANDSHAKE_TASK_ID;
}

/**
This integration test uses hardcoded addresses for executors and the scheduler,
specified in a config file located in the project root directory. In production
systems, these addresses would typically be retrieved from a catalog. This section'
is responsible for parsing the config file.*/

// Format definitions for the config file
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub(crate) scheduler: Scheduler,
    pub(crate) executors: Vec<Executor>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Scheduler {
    pub(crate) id_addr: String,
    pub(crate) port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Executor {
    id: u8,
    ip_addr: String,
    port: u16,
    numa_node: u8,
}

pub fn read_config() -> Config {
    let config_str = fs::read_to_string("executors.toml").expect("Failed to read config file");
    toml::from_str(&config_str).expect("Failed to parse config file")
}


// Starts the scheduler gRPC service
pub async fn start_scheduler_server(addr: &str) {
    let addr = addr.parse().expect("Invalid address");

    println!("Scheduler listening on {}", addr);

    let scheduler_service = SchedulerService::default();
    Server::builder()
        .add_service(SchedulerApiServer::new(scheduler_service))
        .serve(addr)
        .await
        .expect("unable to start scheduler gRPC server");
}

// Compares the results of cur with ref_sol, return true if all record batches are equal
fn is_result_correct(ref_sol: Vec<RecordBatch>, cur: Vec<RecordBatch>) -> bool {
    if ref_sol.len() != cur.len() {
        return false;
    }

    for (ref_batch, cur_batch) in ref_sol.iter().zip(cur.iter()) {
        if ref_batch != cur_batch {
            return false;
        }
    }
    true
}

// ================================
// ====== MOCK EXECUTOR CODE ======
// ================================

// Initialize an executor and load the data from csv
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

// Given an initialized executor and channel, do the initial handshake with the server and return the first task
async fn client_handshake(client: &mut SchedulerApiClient<Channel>) -> NotifyTaskStateRet {
    // Send initial request with handshake task ID
    let handshake_req = tonic::Request::new(NotifyTaskStateArgs {
        task: Some(TaskId {
            query_id: *HANDSHAKE_QUERY_ID,
            task_id: *HANDSHAKE_TASK_ID,
        }),
        success: true,
        result: Vec::new(),
    });

    match client.notify_task_state(handshake_req).await {
        Err(e) => {
            panic!("Error occurred in client handshake: {}", e);
        }

        Ok(response) => {
            let response_inner = response.into_inner();
            assert_eq!(true, response_inner.has_new_task);
            response_inner
        }
    }
}

// Send the results of the current task to scheduler and get the next task to execute
async fn get_next_task(
    args: NotifyTaskStateArgs,
    client: &mut SchedulerApiClient<Channel>,
) -> NotifyTaskStateRet {
    let get_next_task_request = tonic::Request::new(args);

    match client.notify_task_state(get_next_task_request).await {
        Err(e) => {
            panic!("Error occurred in getting next task: {}", e);
        }

        Ok(response) => {
            let response_inner = response.into_inner();
            assert_eq!(true, response_inner.has_new_task);
            response_inner
        }
    }
}

// Starts the executor gRPC service
pub async fn start_executor_client(executor: Executor, scheduler_addr: &str) {
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

    let mock_executor = initialize_executor().await;

    // get the first task by sending handshake message to scheduler
    let mut cur_task = client_handshake(&mut client).await;
    loop {
        assert_eq!(true, cur_task.has_new_task);

        let plan_result = deserialize_physical_plan(cur_task.physical_plan.clone()).await;
        let plan = match plan_result {
            Ok(plan) => plan,
            Err(e) => {
                panic!("Error deserializing physical plan: {:?}", e);
            }
        };

        let execution_result = mock_executor.execute_plan(plan).await;
        let execution_success = execution_result.is_ok();

        // TODO: discuss how to pass result without serialization (how to pass pointer and get access)
        let res = execution_result.unwrap_or_else(|e| Vec::new());
        cur_task = get_next_task(
            NotifyTaskStateArgs {
                task: cur_task.task.clone(),
                success: execution_success,
                result: Vec::new(),
            },
            &mut client,
        )
        .await;
    }
}

// ================================
// ===== END MOCK EXECUTOR CODE ===
// ================================



async fn generate_refsol(file_path: &str) -> Vec<Vec<RecordBatch>> {
    // start a reference executor instance to verify correctness
    let reference_executor = initialize_executor().await;

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
    results
}


// ============================================
// ====== MOCK FRONTEND + OPTIMIZER CODE ======
// ============================================


// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let config = read_config();
//
//     let args: Vec<String> = env::args().collect();
//     // Check if at least one argument was provided (excluding the program name)
//     if args.len() != 1 {
//         panic!("Usage: pass in the .slt file you want to test");
//     }
//
//     let file_path = &args[1];
//     eprintln!("Start running file {}", file_path);
//
//     // Start the scheduler server
//
//     let scheduler_addr = format!("{}:{}", config.scheduler.id_addr, config.scheduler.port);
//
//     let scheduler_addr_for_server = scheduler_addr.clone();
//     tokio::spawn(async move {
//         start_scheduler_server(&scheduler_addr_for_server).await;
//     });
//
//     // Start executor clients
//     for executor in config.executors {
//         // Clone the scheduler_addr for each executor client
//         let scheduler_addr_for_client = scheduler_addr.clone();
//         tokio::spawn(async move {
//             start_executor_client(executor, &scheduler_addr_for_client).await;
//         });
//     }
//
//     // TODO: make this a command line program where it runs a file, verify files by comparing recordbatches
//
//     let test_files = list_all_slt_files("./test_files");
//
//     for file_path in test_files {
//         let file_path_str = file_path
//             .to_str()
//             .expect("Failed to convert path to string");
//
//         eprintln!("Processing test file: {}", file_path_str);
//
//         match get_execution_plan_from_file(file_path_str).await {
//             Ok(plans) => {}
//             Err(e) => {
//                 eprintln!(
//                     "Failed to get execution plans from file {}: {}",
//                     file_path_str, e
//                 );
//                 panic!("Test failed due to error with file: {}", file_path_str);
//             }
//         }
//     }
//
//     Ok(())
// }











#[cfg(test)]
mod tests {
    use crate::integration_test::{
        generate_refsol, initialize_executor, is_result_correct, read_config,
        start_executor_client, start_scheduler_server,
    };
    use crate::mock_executor::DatafusionExecutor;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::ExecutionPlan;
    use std::sync::Arc;

    #[test]
    pub fn test_read_config() {
        let config = read_config();
        assert_eq!("127.0.0.1", config.scheduler.id_addr);
    }
    #[test]
    fn test_is_result_correct_basic() {
        // Handcraft a record batch
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        // Construct another equivalent record batch
        let id_array_2 = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema_2 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch_2 = RecordBatch::try_new(Arc::new(schema_2), vec![Arc::new(id_array_2)]).unwrap();

        assert_eq!(true, is_result_correct(vec![batch], vec![batch_2]));
    }

    #[tokio::test]
    async fn test_is_result_correct_sql() {
        let executor = initialize_executor().await;

        let query = "SELECT * FROM mock_executor_test_table";
        let res_with_sql = executor
            .execute_query(query)
            .await
            .expect("fail to execute sql");

        let plan_result = executor.get_session_context().sql(&query).await;
        let plan = match plan_result {
            Ok(plan) => plan,
            Err(e) => {
                panic!("Failed to create plan: {:?}", e);
            }
        };

        let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
            Ok(plan) => plan,
            Err(e) => {
                panic!("Failed to create physical plan: {:?}", e);
            }
        };

        let result = executor.execute_plan(plan).await;
        assert!(result.is_ok());
        let batches = result.unwrap();

        assert!(!batches.is_empty());

        assert_eq!(true, is_result_correct(res_with_sql, batches));
    }

    #[tokio::test]
    async fn test_handshake() {
        let config = read_config();
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
    }

    #[tokio::test]
    async fn test_generate_refsol() {
        let res = generate_refsol("./test_files/select.slt").await;
        assert_eq!(false, res.is_empty());
        eprintln!("Number of arguments is {}", res.len());
    }
}
