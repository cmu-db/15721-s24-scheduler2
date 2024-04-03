use crate::api::composable_database::scheduler_api_client::SchedulerApiClient;
use crate::api::composable_database::{NotifyTaskStateArgs, NotifyTaskStateRet, TaskId};
use crate::intermediate_results::{insert_results, TaskKey};
use crate::project_config::load_catalog;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::physical_plan_from_bytes;
use futures::stream::StreamExt;
use std::sync::Arc;
use tonic::transport::Channel;

static HANDSHAKE_QUERY_ID: u64 = u64::MAX;
static HANDSHAKE_TASK_ID: u64 = u64::MAX;
static HANDSHAKE_STAGE_ID: u64 = u64::MAX;

pub struct Executor {
    id: i32,
    ctx: SessionContext,
    scheduler: Option<SchedulerApiClient<Channel>>, // api client for the scheduler
}

// TODO: Clean up gRPC calling code.
impl Executor {
    pub async fn new(catalog_path: &str, id: i32) -> Self {
        let ctx = load_catalog(catalog_path).await;
        Self {
            id,
            ctx: (*ctx).clone(),
            scheduler: None,
        }
    }

    pub async fn connect(&mut self, scheduler_addr: &str) {
        println!(
            "[Executor{}]: Connecting to Scheduler at {scheduler_addr}",
            self.id
        );

        let full_address = format!("http://{}", scheduler_addr);
        let server = SchedulerApiClient::connect(full_address)
            .await
            .expect("Failed to connect to Scheduler.");
        self.scheduler = Some(server);

        // get the first task by sending handshake message to scheduler
        let mut cur_task = self.client_handshake().await;
        loop {
            println!("Got new task {:?}", cur_task);
            assert_eq!(true, cur_task.has_new_task);

            let bytes = &cur_task.physical_plan;
            let plan = physical_plan_from_bytes(bytes, &self.ctx)
                .expect("Failed to deserialize physical plan");

            let execution_result = self.execute(plan).await;
            let execution_success = execution_result.is_ok();

            println!("Finish executing, status {}", execution_success);

            if execution_success {
                let result = execution_result.unwrap();

                let cur_task_inner = cur_task.task.clone().unwrap();
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

    // Given an initialized executor and channel, do the initial handshake with the server and return the first task
    pub async fn client_handshake(&mut self) -> NotifyTaskStateRet {
        assert!(self.scheduler.is_some());
        let client: &mut SchedulerApiClient<Channel> = self
            .scheduler
            .as_mut()
            .expect("Client is expected to be initialized");

        // Send initial request with handshake task ID
        let handshake_req = tonic::Request::new(NotifyTaskStateArgs {
            task: Some(TaskId {
                query_id: HANDSHAKE_QUERY_ID,
                stage_id: HANDSHAKE_STAGE_ID,
                task_id: HANDSHAKE_TASK_ID,
            }),
            success: true,
            result: Vec::new(),
        });

        match client.notify_task_state(handshake_req).await {
            Err(e) => {
                panic!("Error occurred in client handshake: {}", e);
            }

            Ok(response) => {
                println!("Executor handshake success");
                let response_inner = response.into_inner();
                assert_eq!(true, response_inner.has_new_task);
                response_inner
            }
        }
    }

    // Send the results of the current task to scheduler and get the next task to execute
    async fn get_next_task(&mut self, args: NotifyTaskStateArgs) -> NotifyTaskStateRet {
        println!("Sending message back to scheduler {:?}", args);
        assert!(self.scheduler.is_some());
        let client = self
            .scheduler
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

    pub async fn execute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = self.ctx.task_ctx();
        let mut results = Vec::new();
        let mut stream = plan.execute(0, task_ctx)?;
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }
        Ok(results)
    }

    #[allow(dead_code)]
    pub async fn execute_sql(&self, query: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
        // NOTE: More direct way to execute SQL, using below to ensure same code paths are taken.
        // self.ctx
        //     .sql(query)
        //     .await
        //     .expect("Failed to parse SQL statement")
        //     .collect()
        //     .await

        let physical_plan = self
            .ctx
            .sql(query)
            .await
            .expect("Failed to parse SQL statement")
            .create_physical_plan()
            .await
            .expect("Failed to create physical plan");
        self.execute(physical_plan).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_DATA_PATH: &str = "./test_files/";

    #[tokio::test]
    async fn test_execute_sql() {
        let executor = Executor::new(TEST_DATA_PATH, 0).await;

        let query = "SELECT * FROM mock_executor_test_table";
        let result = executor.execute_sql(query).await;

        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
