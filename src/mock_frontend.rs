use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use tonic::transport::Channel;
use crate::api::composable_database::scheduler_api_server::SchedulerApi;
use crate::api::composable_database::{
    QueryInfo, ScheduleQueryArgs
};

use crate::api::composable_database::scheduler_api_client::SchedulerApiClient;

use crate::parser::Parser;
use datafusion::error::Result;
pub struct MockFrontend {
    parser: Parser,
    // Sender for sending SQL execution requests to the background task
    sender: mpsc::Sender<SqlExecutionRequest>
}

// Structure to encapsulate SQL execution requests
struct SqlExecutionRequest {
    plan: Arc<dyn ExecutionPlan>,
    response: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

impl MockFrontend {
    pub async fn new(catalog_path: &str, scheduler_addr: &str) -> Self {
        let channel = Channel::from_shared(scheduler_addr.to_string())
            .expect("Invalid scheduler address")
            .connect()
            .await
            .expect("Failed to connect to scheduler");

        let mut client = SchedulerApiClient::new(channel);
        let parser = Parser::new(catalog_path).await;

        let (sender, mut receiver) = mpsc::channel::<SqlExecutionRequest>(32);

        // Background task for handling SQL execution
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {

                let plan_bytes = parser.serialize_physical_plan(request.plan).await?;
                let schedule_query_request = tonic::Request::new(ScheduleQueryArgs{
                    physical_plan: plan_bytes,
                    metadata: Some(QueryInfo {
                        priority: 0,
                        cost: 0,
                    })
                });
                // Simulating a gRPC call
                match client.schedule_query(schedule_query_request).await {
                    Ok(response) => {

                        let _ = request.response.send(Ok(vec![])); // Replace vec![] with actual processing of response
                    },
                    Err(e) => {
                        let _ = request.response.send(Err(DataFusionError::Execution(format!("Failed to execute query: {}", e))));
                    },
                }
            }
        });

        Self {
            parser,
            sender,
        }
    }

    pub async fn run_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = self.parser.sql_to_physical_plan(sql)?.to_logical_plan()?;
        let plan = self.parser.optimize(plan)?;
        let physical_plan = self.parser.to_physical_plan(plan)?;

        let (response_tx, response_rx) = oneshot::channel();
        let request = SqlExecutionRequest {
            plan: Arc::new(physical_plan),
            response: response_tx,
        };

        self.sender.send(request).await.expect("Failed to send request to background task");

        response_rx.await.expect("Failed to receive response from background task")
    }
}
