use crate::intermediate_results::{insert_results, rewrite_query, TaskKey};
use crate::mock_catalog::load_catalog;
use crate::mock_executor::MockExecutor;
use crate::server::composable_database::scheduler_api_client::SchedulerApiClient;
use crate::server::composable_database::{NotifyTaskStateArgs, NotifyTaskStateRet, TaskId};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::physical_plan_from_bytes;
use tonic::transport::Channel;

const HANDSHAKE_QUERY_ID: u64 = u64::MAX;
const HANDSHAKE_TASK_ID: u64 = u64::MAX;
const HANDSHAKE_STAGE_ID: u64 = u64::MAX;

pub struct ExecutorClient {
    id: i32,
    ctx: SessionContext,
    scheduler: Option<SchedulerApiClient<Channel>>, // api client for the scheduler
    executor: MockExecutor,
}

// TODO: Clean up gRPC calling code.
impl ExecutorClient {
    pub async fn new(catalog_path: &str, id: i32) -> Self {
        let ctx = load_catalog(catalog_path).await;
        Self {
            id,
            ctx: (*ctx).clone(),
            scheduler: None,
            executor: MockExecutor::new(catalog_path).await,
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
            assert_eq!(true, cur_task.has_new_task);

            let cur_task_inner = cur_task.task.clone().unwrap();

            let bytes = &cur_task.physical_plan;
            let plan = physical_plan_from_bytes(bytes, &self.ctx)
                .expect("Failed to deserialize physical plan");

            // Rewrite the query plan to attach intermediate data
            let plan = rewrite_query(plan, cur_task_inner.query_id)
                .await
                .expect("fail to rewrite query");

            let execution_result = self.executor.execute(plan).await;
            let execution_success = execution_result.is_ok();

            if execution_success {
                let result = execution_result.unwrap();

                // insert intermediate results into intermediate result hashmap
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
}
