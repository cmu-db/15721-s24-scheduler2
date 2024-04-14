use crate::server::composable_database::{QueryInfo, ScheduleQueryArgs};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use std::thread::sleep;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tonic::transport::Channel;

use crate::server::composable_database::scheduler_api_client::SchedulerApiClient;

use crate::parser::ExecutionPlanParser;
use crate::server::composable_database::QueryJobStatusArgs;
use crate::server::composable_database::QueryStatus;
use datafusion::error::Result;

pub struct MockFrontend {
    parser: ExecutionPlanParser,
    // Sender for sending SQL execution requests to the background task
    sender: mpsc::Sender<SqlExecutionRequest>,
}

// Structure to encapsulate SQL execution requests
struct SqlExecutionRequest {
    plan: Arc<dyn ExecutionPlan>,
    response: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

impl MockFrontend {
    pub async fn new(catalog_path: &str, scheduler_addr: &str) -> Self {
        let full_address = format!("http://{}", scheduler_addr);
        println!("The full address is {}", full_address);

        let mut client = SchedulerApiClient::connect("http://0.0.0.0:15721")
            .await
            .expect("Fail to connect to scheduler");

        let (sender, mut receiver) = mpsc::channel::<SqlExecutionRequest>(32);

        // Clone `catalog_path` and convert to `String` for ownership transfer to the async block.
        let catalog_path_owned = catalog_path.to_owned();

        // Move the owned String into the async block
        tokio::spawn(async move {
            let parser = ExecutionPlanParser::new(&catalog_path_owned).await;

            while let Some(request) = receiver.recv().await {
                println!("Got request from frontend {:?}", request.plan);
                let plan_bytes = parser
                    .serialize_physical_plan(request.plan)
                    .expect("MockFrontend: fail to parse physical plan");
                let schedule_query_request = tonic::Request::new(ScheduleQueryArgs {
                    physical_plan: plan_bytes,
                    metadata: Some(QueryInfo {
                        priority: 0,
                        cost: 0,
                    }),
                });
                // TODO: how does the scheduler store the query and return the result?
                match client.schedule_query(schedule_query_request).await {
                    Ok(response) => {
                        let query_id = response.into_inner().query_id;
                        println!(
                            "FrontEnd: Got response from scheduler, query id is {}",
                            query_id
                        );

                        loop {
                            let status = client
                                .query_job_status(tonic::Request::new(QueryJobStatusArgs {
                                    query_id,
                                }))
                                .await
                                .expect("invalid query job status response")
                                .into_inner()
                                .query_status;

                            println!("Status of current query is {:?}", status);

                            if status == QueryStatus::Done as i32 {
                                break;
                            }

                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }

                        let _ = request.response.send(Ok(vec![])); // Replace vec![] with actual processing of response
                    }
                    Err(e) => {
                        let _ = request
                            .response
                            .send(Err(DataFusionError::Execution(format!(
                                "Failed to execute query: {}",
                                e
                            ))));
                    }
                }
            }
        });

        Self {
            parser: ExecutionPlanParser::new(catalog_path).await,
            sender,
        }
    }

    pub async fn run_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let physical_plan = self.parser.sql_to_physical_plan(sql).await?;

        let (response_tx, response_rx) = oneshot::channel();
        let request = SqlExecutionRequest {
            plan: physical_plan,
            response: response_tx,
        };

        println!("Request sent to the background thread");

        self.sender
            .send(request)
            .await
            .expect("Failed to send request to background task");

        response_rx
            .await
            .expect("Failed to receive response from background task")
    }
}
