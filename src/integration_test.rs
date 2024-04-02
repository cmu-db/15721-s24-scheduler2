use crate::api::composable_database::scheduler_api_client::SchedulerApiClient;
use crate::api::composable_database::scheduler_api_server::SchedulerApiServer;
use crate::api::composable_database::{NotifyTaskStateArgs, ScheduleQueryArgs};
use crate::api::composable_database::{NotifyTaskStateRet, TaskId};
use crate::api::SchedulerService;
use crate::mock_executor::DatafusionExecutor;
use crate::parser::{deserialize_physical_plan, get_execution_plan_from_file, list_all_slt_files};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use tonic::transport::{Channel, Server};
use walkdir::WalkDir;
use std::sync::Arc;
use crate::project_config::{read_config, load_catalog};
use crate::project_config::{Config, Executor};


/**
This gRPC facilitates communication between executors and the scheduler:
rpc NotifyTaskState(NotifyTaskStateArgs) returns (NotifyTaskStateRet);

Executor to scheduler message:
message NotifyTaskStateArgs {
    TaskID task = 1; // Task identifier
    bool success = 2; // Indicates if the task was executed successfully
    bytes result = 3; // Result data as bytes
}
- Notifies the scheduler of the task's execution xwstatus using the associated TaskID and transmits the result data.

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
    static ref HANDSHAKE_STAGE_ID: u64 = -1i64 as u64;
    static ref HANDSHAKE_TASK: TaskId = TaskId {
        query_id: *HANDSHAKE_QUERY_ID,
        stage_id: *HANDSHAKE_STAGE_ID,
        task_id: *HANDSHAKE_TASK_ID,
    };
}

struct IntegrationTest {
    catalog_path: String,
    config_path: String,
    ctx: Arc<SessionContext>,
    config: Config
}


/**
This integration test uses hardcoded addresses for executors and the scheduler,
specified in a config file located in the project root directory. In production
systems, these addresses would typically be retrieved from a catalog. This section'
is responsible for parsing the config file.*/



// Starts the scheduler gRPC service
pub async fn start_scheduler_server(addr: &str) {
    let addr = addr.parse().expect("Invalid address");

    println!("Scheduler listening on {}", addr);

    let scheduler_service = SchedulerService::new();
    Server::builder()
        .add_service(SchedulerApiServer::new(scheduler_service))
        .serve(addr)
        .await
        .expect("unable to start scheduler gRPC server");
}


impl IntegrationTest {

    // Given the paths to the catalog (containing all db files) and a path to the config file,
    // create a new instance of IntegrationTester
    pub async fn new(catalog_path: String, config_path: String) -> Self {
        let ctx = load_catalog(&catalog_path).await;
        let config = read_config(&config_path);

        Self {
            ctx,
            config,
            catalog_path,
            config_path
        }
    }

    pub async fn run_server(&self) {
        let scheduler_addr = format!("{}:{}", self.config.scheduler.id_addr, self.config.scheduler.port);
        let scheduler_addr_for_server = scheduler_addr.clone();
        tokio::spawn(async move {
            start_scheduler_server(&scheduler_addr_for_server).await;
        });
    }
    pub async fn run_client(&self) {
        let scheduler_addr = format!("{}:{}", self.config.scheduler.id_addr, self.config.scheduler.port);
        // Start executor clients
        for executor in &self.config.executors {
            // Clone the scheduler_addr for each executor client
            let mut mock_executor = DatafusionExecutor::new("./test_files/", executor.id).await;
            let scheduler_addr_copy = scheduler_addr.clone();
            tokio::spawn(async move {
                mock_executor.run_mock_executor_service(&scheduler_addr_copy).await;
            });
        }
    }
}




// TODO for next update: logic for verifying correctness

// Compares the results of cur with ref_sol, return true if all record batches are equal
// fn is_result_correct(ref_sol: Vec<RecordBatch>, cur: Vec<RecordBatch>) -> bool {
//     if ref_sol.len() != cur.len() {
//         return false;
//     }
//
//     for (ref_batch, cur_batch) in ref_sol.iter().zip(cur.iter()) {
//         if ref_batch != cur_batch {
//             return false;
//         }
//     }
//     true
// }

// async fn generate_refsol(file_path: &str) -> Vec<Vec<RecordBatch>> {
//     // start a reference executor instance to verify correctness
//     let reference_executor = initialize_executor().await;
//
//     // get all the execution plans and pre-compute all the reference results
//     let execution_plans = get_execution_plan_from_file(file_path)
//         .await
//         .expect("Failed to get execution plans");
//     let mut results = Vec::new();
//     for plan in execution_plans {
//         match reference_executor.execute_plan(plan).await {
//             Ok(dataframe) => {
//                 results.push(dataframe);
//             }
//             Err(e) => eprintln!("Failed to execute plan: {}", e),
//         }
//     }
//     results
//
// }


#[cfg(test)]
mod tests {
    // use crate::mock_executor::DatafusionExecutor;
    // use datafusion::arrow::array::Int32Array;
    // use datafusion::arrow::datatypes::{DataType, Field, Schema};
    // use datafusion::arrow::record_batch::RecordBatch;
    // use datafusion::physical_plan::ExecutionPlan;
    // use std::sync::Arc;
    // use datafusion::physical_plan::test::exec::MockExec;

    // #[test]
    // fn test_is_result_correct_basic() {
    //     // Handcraft a record batch
    //     let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    //     let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    //
    //     let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    //
    //     // Construct another equivalent record batch
    //     let id_array_2 = Int32Array::from(vec![1, 2, 3, 4, 5]);
    //     let schema_2 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    //
    //     let batch_2 = RecordBatch::try_new(Arc::new(schema_2), vec![Arc::new(id_array_2)]).unwrap();
    //
    //     assert_eq!(true, is_result_correct(vec![batch], vec![batch_2]));
    // }

    // #[tokio::test]
    // async fn test_is_result_correct_sql() {
    //     let executor = DatafusionExecutor::new("./test_files/");
    //
    //     let query = "SELECT * FROM mock_executor_test_table";
    //     let res_with_sql = executor
    //         .execute_query(query)
    //         .await
    //         .expect("fail to execute sql");
    //
    //     let plan_result = executor.get_session_context().sql(&query).await;
    //     let plan = match plan_result {
    //         Ok(plan) => plan,
    //         Err(e) => {
    //             panic!("Failed to create plan: {:?}", e);
    //         }
    //     };
    //
    //     let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
    //         Ok(plan) => plan,
    //         Err(e) => {
    //             panic!("Failed to create physical plan: {:?}", e);
    //         }
    //     };
    //
    //     let result = executor.execute_plan(plan).await;
    //     assert!(result.is_ok());
    //     let batches = result.unwrap();
    //
    //     assert!(!batches.is_empty());
    //
    //     assert_eq!(true, is_result_correct(res_with_sql, batches));
    // }

    // #[tokio::test]
    // async fn test_handshake() {
    //     let config = read_config();
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
    // }

    // #[tokio::test]
    // async fn test_generate_refsol() {
    //     let res = generate_refsol("./test_files/select.slt").await;
    //     assert_eq!(false, res.is_empty());
    //     eprintln!("Number of arguments is {}", res.len());
    // }
}