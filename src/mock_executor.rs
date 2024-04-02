use crate::api::composable_database::scheduler_api_client::SchedulerApiClient;
use crate::api::composable_database::{NotifyTaskStateArgs, NotifyTaskStateRet, TaskId};
use crate::intermediate_results::{insert_results, TaskKey};
use crate::parser::deserialize_physical_plan;
use crate::project_config::load_catalog;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::CsvReadOptions;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use std::sync::Arc;
use tonic::transport::Channel;

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

pub struct DatafusionExecutor {
    ctx: Arc<SessionContext>,
    id: i32,
    client: Option<SchedulerApiClient<Channel>>, // api client for the scheduler
}

impl DatafusionExecutor {
    pub async fn new(catalog_path: &str, id: i32) -> Self {
        Self {
            ctx: load_catalog(catalog_path).await,
            id,
            client: None,
        }
    }

    // Function to execute a query from a SQL string
    pub async fn execute_query(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = match self.ctx.sql(query).await {
            Ok(dataframe) => dataframe,
            Err(e) => {
                eprintln!("Error executing query: {}", e);
                return Err(e);
            }
        };

        match df.collect().await {
            Ok(result) => Ok(result),
            Err(e) => {
                eprintln!("Error collecting results: {}", e);
                Err(e)
            }
        }
    }

    // Function to execute a query from an ExecutionPlan
    pub async fn execute_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = self.ctx.task_ctx();
        let mut batches = Vec::new();

        match plan.execute(0, task_ctx) {
            Ok(mut stream) => {
                // Iterate over the stream
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(record_batch) => {
                            batches.push(record_batch);
                        }
                        Err(e) => {
                            eprintln!("Error processing batch: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
            Err(e) => eprintln!("Failed to execute plan: {}", e),
        }
        Ok(batches)
    }

    pub fn get_session_context(&self) -> Arc<SessionContext> {
        self.ctx.clone()
    }

    // Given an initialized executor and channel, do the initial handshake with the server and return the first task
    pub async fn client_handshake(&mut self) -> NotifyTaskStateRet {
        assert!(self.client.is_some());
        let client: &mut SchedulerApiClient<Channel> = self
            .client
            .as_mut()
            .expect("Client is expected to be initialized");

        // Send initial request with handshake task ID
        let handshake_req = tonic::Request::new(NotifyTaskStateArgs {
            task: Some(TaskId {
                query_id: *HANDSHAKE_QUERY_ID,
                stage_id: *HANDSHAKE_STAGE_ID,
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
    async fn get_next_task(&mut self, args: NotifyTaskStateArgs) -> NotifyTaskStateRet {
        assert!(self.client.is_some());
        let client = self
            .client
            .as_mut()
            .expect("Client is expected to be initialized");

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

    pub async fn run_mock_executor_service(&mut self, scheduler_addr: &str) {
        println!("Executor {} connecting to scheduler", self.id);

        // Create a connection to the scheduler
        let channel = Channel::from_shared(scheduler_addr.to_string())
            .expect("Invalid scheduler address")
            .connect()
            .await
            .expect("Failed to connect to scheduler");

        // Create a client using the channel
        self.client = Some(SchedulerApiClient::new(channel));

        // get the first task by sending handshake message to scheduler
        let mut cur_task = self.client_handshake().await;
        loop {
            assert_eq!(true, cur_task.has_new_task);

            let plan_result = deserialize_physical_plan(cur_task.physical_plan.clone()).await;
            let plan = match plan_result {
                Ok(plan) => plan,
                Err(e) => {
                    panic!("Error deserializing physical plan: {:?}", e);
                }
            };

            let cur_task_inner = cur_task.task.clone().unwrap();

            let execution_result = self.execute_plan(plan).await;
            let execution_success = execution_result.is_ok();

            // TODO: discuss how to pass result without serialization (how to pass pointer and get access)

            if execution_success {
                let result = execution_result.unwrap();
                insert_results(
                    TaskKey {
                        stage_id: cur_task_inner.stage_id,
                        query_id: cur_task_inner.query_id,
                    },
                    result,
                )
                .await;
            }

            cur_task = self
                .get_next_task(NotifyTaskStateArgs {
                    task: cur_task.task.clone(),
                    success: execution_success,
                    result: Vec::new(),
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    // Helper function to create a DatafusionExecutor instance
    async fn create_executor() -> DatafusionExecutor {
        let executor = DatafusionExecutor::new("./test_files/", 0);
        let table_name = "mock_executor_test_table";
        let file_path = "./test_files/mock_executor_test_table.csv"; // Ensure this file exists in the test environment
        let options = CsvReadOptions::new();
        let result = executor.register_csv(table_name, file_path, options).await;
        assert!(result.is_ok());
        executor
    }

    // Test registering a CSV file
    #[tokio::test]
    async fn test_register_csv() {
        let _ = create_executor();
    }

    // Test running a SQL query
    #[tokio::test]
    async fn test_execute_sql_query() {
        let executor = create_executor();

        let query = "SELECT * FROM mock_executor_test_table";
        let result = executor.await.execute_query(query).await;

        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 2);
    }

    // Test executing a plan
    #[tokio::test]
    async fn test_execute_plan() {
        let executor = create_executor().await;

        let query = "SELECT * FROM mock_executor_test_table";

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
        assert!(!batches.is_empty()); // Ensure that we get some results
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
