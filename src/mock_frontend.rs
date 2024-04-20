use crate::server::composable_database::{QueryInfo, ScheduleQueryArgs};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{HashMap, HashSet};
use std::os::linux::raw::stat;
use std::sync::Arc;
use std::thread::sleep;
use tokio::sync::{mpsc, oneshot, Mutex};
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
    scheduler_api_client: Option<SchedulerApiClient<Channel>>,
    jobs: Arc<Mutex<HashMap<u64, JobInfo>>>,
}

impl MockFrontend {
    pub(crate) async fn run_polling_task(shared_frontend: Arc<Mutex<MockFrontend>>) {
        let polling_period_ms = 1000;
        let mut interval = tokio::time::interval(Duration::from_millis(polling_period_ms));
        loop {
            interval.tick().await;
            let mut frontend = shared_frontend.lock().await;
            let results = frontend.poll_results().await;
            println!("Poll results: {:?}", results);
            // Drop the lock manually if needed or it will be dropped at the end of the block
        }
    }

    pub async fn new(catalog_path: &str) -> Self {
        let ctx = load_catalog(catalog_path).await;
        let jobs = HashMap::new();
        let mutex = Mutex::new(jobs);
        let arc = Arc::new(mutex);

        Self {
            optimizer: Optimizer::new(catalog_path).await,
            parser: ExecutionPlanParser::new(catalog_path).await,
            jobs: arc,
            ctx: (*ctx).clone(),
            scheduler_api_client: None,
        }
    }

    pub async fn connect(&mut self, scheduler_addr: &str) {
        let full_address = format!("http://{}", scheduler_addr);
        println!("Connecting to scheduler at {}", full_address);

        let client = SchedulerApiClient::connect(full_address)
            .await
            .expect("Failed to connect to scheduler");

        self.scheduler_api_client = Some(client);
    }

    // creates a logical plan for the sql string
    pub async fn sql_to_logical_plan(&self, sql_string: &str) -> Result<LogicalPlan> {
        self.ctx.state().create_logical_plan(sql_string).await
    }

    // submit a new query to the scheduler, returns the query id for this query
    pub async fn submit_job(&mut self, sql_string: &str) -> Result<u64, DataFusionError> {
        assert!(self.scheduler_api_client.is_some());
        let mut jobs = self.jobs.lock().await;

        let logical_plan = self.sql_to_logical_plan(sql_string).await?;
        let physical_plan = self.optimizer.optimize(&logical_plan).await?;
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

        let mut client = self.scheduler_api_client.as_mut().unwrap();

        match client.schedule_query(schedule_query_request).await {
            Ok(response) => {
                let query_id = response.into_inner().query_id;
                let existing_value = jobs.insert(
                    query_id,
                    JobInfo {
                        submitted_at: time::Instant::now(),
                        sql_string: sql_string.to_string(),
                        result: None,
                        finished_at: None,
                        status: QueryStatus::InProgress,
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

    pub async fn poll_results(&mut self) -> HashSet<u64> {
        assert!(self.scheduler_api_client.is_some());

        let mut jobs = self.jobs.lock().await;
        let mut res: HashSet<u64> = HashSet::new();
        let mut client = self.scheduler_api_client.as_mut().unwrap();

        let keys: Vec<u64> = jobs.keys().cloned().collect();
        for query_id in keys {
            let job = jobs.get(&query_id).unwrap_or_else(|| {
                panic!("poll_results: job for query id {} does not exist", query_id)
            });

            // Send request to scheduler only when job is in progress
            if job.status != InProgress {
                continue;
            }

            let status = match client
                .query_job_status(tonic::Request::new(QueryJobStatusArgs { query_id }))
                .await
            {
                Ok(response) => response.into_inner(),
                Err(err) => {
                    eprintln!(
                        "poll_results: fail to get job status for query id {}: {}",
                        query_id, err
                    );
                    continue;
                }
            };

            let new_query_status = QueryStatus::try_from(status.query_status)
                .expect("poll results: fail to decode query status");
            match new_query_status {
                QueryStatus::Done => {
                    res.insert(query_id);

                    let serialized_results = status.query_result;

                    let results =
                        match ExecutionPlanParser::deserialize_record_batch(serialized_results) {
                            Ok(res) => res,
                            Err(err) => {
                                eprintln!(
                                    "poll_results: fail to deserialize results for query {}: {}",
                                    query_id, err
                                );
                                continue;
                            }
                        };

                    let updated_job_info = JobInfo {
                        status: QueryStatus::Done,
                        finished_at: Some(time::Instant::now()),
                        result: Some(results),
                        submitted_at: job.submitted_at,
                        sql_string: job.sql_string.to_string(),
                    };

                    let v = jobs.insert(query_id, updated_job_info);
                    // original value must already exist in the map
                    assert!(!v.is_none());
                }

                QueryStatus::NotFound => {
                    panic!(
                        "poll_results: query id {} not found from the scheduler",
                        query_id
                    );
                }

                QueryStatus::Failed => {
                    res.insert(query_id);
                    let updated_job_info = JobInfo {
                        status: QueryStatus::Failed,
                        finished_at: Some(time::Instant::now()),
                        result: None,
                        submitted_at: job.submitted_at,
                        sql_string: job.sql_string.to_string(),
                    };
                    let v = jobs.insert(query_id, updated_job_info);
                    // original value must already exist in the map
                    assert!(!v.is_none());
                }
                _ => {
                    panic!("poll_results: invalid query status: {:?}", new_query_status);
                }
            }
        }
        res
    }

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
