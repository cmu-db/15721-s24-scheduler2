use crate::server::composable_database::{QueryInfo, ScheduleQueryArgs};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{HashMap, HashSet};
use std::os::linux::raw::stat;
use std::sync::Arc;
use std::thread::sleep;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tonic::transport::Channel;

use crate::server::composable_database::scheduler_api_client::SchedulerApiClient;

use crate::mock_optimizer::Optimizer;
use crate::parser::ExecutionPlanParser;
use crate::project_config::{load_catalog, SchedulerConfig};
use crate::server::composable_database::QueryJobStatusArgs;
use crate::server::composable_database::QueryStatus;
use crate::server::composable_database::QueryStatus::InProgress;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sqlparser::parser::Parser;

struct JobInfo {
    sql_string: String,
    status: QueryStatus,
    submitted_at: time::Instant,
    finished_at: Option<time::Instant>,
    result: Option<Vec<RecordBatch>>,
}

pub struct MockFrontend {
    optimizer: Optimizer,
    ctx: SessionContext,
    parser: ExecutionPlanParser,

    // gRPC client for the scheduler
    scheduler_api_client: SchedulerApiClient<Channel>,
    jobs: HashMap<u64, JobInfo>,
}

impl MockFrontend {
    pub async fn new(catalog_path: &str, scheduler_addr: &str) -> Self {
        let ctx = load_catalog(catalog_path).await;
        let full_address = format!("http://{}", scheduler_addr);
        println!("The full address is {}", full_address);

        let client = SchedulerApiClient::connect(&full_address)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "MockFrontEnd: fail to connect to scheduler at addr {}",
                    full_address
                );
            });

        Self {
            optimizer: Optimizer::new(catalog_path),
            parser: ExecutionPlanParser::new(catalog_path),
            jobs: HashMap::new(),
            ctx: (*ctx).clone(),
            scheduler_api_client: client,
        }
    }

    // creates a logical plan for the sql string
    pub fn sql_to_logical_plan(&self, sql_string: &str) -> Result<LogicalPlan> {
        self.ctx.state().create_logical_plan(sql_string)
    }

    // submit a new query to the scheduler, returns the query id for this query
    pub async fn submit_job(&mut self, sql_string: &str) -> Result<u64> {
        let logical_plan = self.sql_to_logical_plan(sql_string)?;
        let physical_plan = self.optimizer.optimize(&logical_plan)?;
        let plan_bytes = self
            .parser
            .serialize_physical_plan(physical_plan)
            .expect("MockFrontend: fail to parse physical plan");

        let schedule_query_request = tonic::Request::new(ScheduleQueryArgs {
            physical_plan: plan_bytes,
            metadata: Some(QueryInfo {
                priority: 0,
                cost: 0,
            }),
        });

        match self
            .scheduler_api_client
            .schedule_query(schedule_query_request)
            .await
        {
            Ok(response) => {
                let query_id = response.into_inner().query_id;
                let existing_value = self.jobs.insert(
                    query_id,
                    JobInfo {
                        submitted_at: time::Instant::now(),
                        sql_string: sql_string.to_string(),
                        result: None,
                        finished_at: None,
                        status: InProgress,
                    },
                );
                // The new query cannot already be in the hashmap of jobs
                assert!(existing_value.is_none());
                Ok(query_id)
            }

            Err(e) => {
                eprintln!("fail to scheduler query {}: {:?}", sql_string, e);
                Err(DataFusionError::Internal(e.to_string()))
            }
        }
    }

    // pull
    pub async fn poll_results(&mut self) -> HashSet<u64> {
        let mut res: HashSet<u64> = HashSet::new();
        for (query_id, job) in self.jobs {
            // no need to check with scheduler if job already finished
            if job.status != InProgress {
                continue;
            }

            let status = self
                .scheduler_api_client
                .query_job_status(tonic::Request::new(QueryJobStatusArgs { query_id }))
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "poll_results: fail to get job status for query id {}",
                        query_id
                    );
                })
                .into_inner();

            match status.query_status.into() {
                InProgress => {}
                Done => {
                    res.insert(query_id);
                    let updated_job_info = JobInfo {
                        status: Done,
                        finished_at: Some(time::Instant::now()),
                        result: Some(status.query_result.clone()),
                        ..*job
                    };
                }
            };
        }
        res
    }

    // pub async fn new(catalog_path: &str, scheduler_addr: &str) -> Self {
    //     let full_address = format!("http://{}", scheduler_addr);
    //     println!("The full address is {}", full_address);
    //
    //     let mut client = SchedulerApiClient::connect(&full_address)
    //         .await
    //         .expect("Fail to connect to scheduler");
    //
    //     let (sender, mut receiver) = mpsc::channel::<SqlExecutionRequest>(32);
    //
    //     // Clone `catalog_path` and convert to `String` for ownership transfer to the async block.
    //     let catalog_path_owned = catalog_path.to_owned();
    //
    //     // Move the owned String into the async block
    //     tokio::spawn(async move {
    //         let parser = ExecutionPlanParser::new(&catalog_path_owned).await;
    //
    //         while let Some(request) = receiver.recv().await {
    //             println!("Got request from frontend {:?}", request.plan);
    //             let plan_bytes = parser
    //                 .serialize_physical_plan(request.plan)
    //                 .expect("MockFrontend: fail to parse physical plan");
    //             let schedule_query_request = tonic::Request::new(ScheduleQueryArgs {
    //                 physical_plan: plan_bytes,
    //                 metadata: Some(QueryInfo {
    //                     priority: 0,
    //                     cost: 0,
    //                 }),
    //             });
    //             // TODO: how does the scheduler store the query and return the result?
    //             match client.schedule_query(schedule_query_request).await {
    //                 Ok(response) => {
    //                     let query_id = response.into_inner().query_id;
    //                     println!(
    //                         "FrontEnd: Got response from scheduler, query id is {}",
    //                         query_id
    //                     );
    //
    //                     loop {
    //                         let status = client
    //                             .query_job_status(tonic::Request::new(QueryJobStatusArgs {
    //                                 query_id,
    //                             }))
    //                             .await
    //                             .expect("invalid query job status response")
    //                             .into_inner()
    //                             .query_status;
    //
    //                         println!("Status of current query is {:?}", status);
    //
    //                         if status == QueryStatus::Done as i32 {
    //                             break;
    //                         }
    //
    //                         tokio::time::sleep(Duration::from_millis(500)).await;
    //                     }
    //
    //                     let _ = request.response.send(Ok(vec![])); // Replace vec![] with actual processing of response
    //                 }
    //                 Err(e) => {
    //                     let _ = request
    //                         .response
    //                         .send(Err(DataFusionError::Execution(format!(
    //                             "Failed to execute query: {}",
    //                             e
    //                         ))));
    //                 }
    //             }
    //         }
    //     });
    //
    //     Self {
    //         parser: ExecutionPlanParser::new(catalog_path).await,
    //         sender,
    //     }
    // }

    // pub async fn run_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
    //     let physical_plan = self.parser.sql_to_physical_plan(sql).await?;
    //
    //     let (response_tx, response_rx) = oneshot::channel();
    //     let request = SqlExecutionRequest {
    //         plan: physical_plan,
    //         response: response_tx,
    //     };
    //
    //     println!("Request sent to the background thread");
    //
    //     self.sender
    //         .send(request)
    //         .await
    //         .expect("Failed to send request to background task");
    //
    //     response_rx
    //         .await
    //         .expect("Failed to receive response from background task")
    // }
}
