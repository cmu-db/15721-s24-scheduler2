use crate::server::composable_database::{QueryInfo, ScheduleQueryArgs};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::{pretty_format_batches, print_batches};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{HashMap, HashSet};
use std::fmt;
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

#[derive(Clone)]
pub struct JobInfo {
    pub sql_string: String,
    pub status: QueryStatus,
    pub submitted_at: time::Instant,
    pub finished_at: Option<time::Instant>,
    pub result: Option<RecordBatch>,
}
impl fmt::Display for JobInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let submitted_at = self.submitted_at.elapsed().as_secs();
        let finished_at = self
            .finished_at
            .map(|t| t.elapsed().as_secs())
            .map_or_else(|| "not finished".to_string(), |secs| secs.to_string());

        let result_summary = if let Some(ref batch) = self.result {
            match pretty_format_batches(&[batch.clone()]) {
                // Assuming you can clone or have a reference
                Ok(formatted_batches) => formatted_batches.to_string(),
                Err(e) => format!("Error formatting results: {}", e),
            }
        } else {
            "No result".to_string()
        };

        write!(
            f,
            "SQL Query: '{}', Status: {:?}, Submitted: {} seconds ago, Finished: {}, Result: {}",
            self.sql_string, self.status, submitted_at, finished_at, result_summary
        )
    }
}

pub struct MockFrontend {
    optimizer: Optimizer,
    ctx: SessionContext,
    parser: ExecutionPlanParser,

    // gRPC client for the scheduler
    scheduler_api_client: Option<SchedulerApiClient<Channel>>,
    jobs: HashMap<u64, JobInfo>,

    // counters
    num_running_jobs: u64,
    num_finished_jobs: u64,
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
        Self {
            optimizer: Optimizer::new(catalog_path).await,
            parser: ExecutionPlanParser::new(catalog_path).await,
            jobs: HashMap::new(),
            ctx: (*ctx).clone(),
            scheduler_api_client: None,
            num_finished_jobs: 0,
            num_running_jobs: 0,
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

        let logical_plan = self.sql_to_logical_plan(sql_string).await?;
        let physical_plan = self.optimizer.optimize(&logical_plan).await?;
        let plan_bytes = self
            .parser
            .serialize_physical_plan(physical_plan.clone())
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
                self.num_running_jobs += 1;
                Ok(query_id)
            }

            Err(e) => {
                eprintln!("fail to scheduler query {}: {:?}", sql_string, e);
                Err(DataFusionError::Internal(e.to_string()))
            }
        }
    }

    // check status of a job
    pub async fn check_job_status(&mut self, query_id: u64) -> Option<&JobInfo> {
        return self.jobs.get(&query_id);
    }

    pub async fn get_num_running_jobs(&self) -> u64 {
        assert_eq!(
            self.jobs.len() as u64,
            self.num_finished_jobs + self.num_running_jobs
        );
        self.num_running_jobs
    }

    pub async fn get_num_finished_jobs(&self) -> u64 {
        assert_eq!(
            self.jobs.len() as u64,
            self.num_finished_jobs + self.num_running_jobs
        );
        self.num_finished_jobs
    }

    pub fn get_all_jobs(&self) -> HashMap<u64, JobInfo> {
        self.jobs.clone()
    }

    async fn poll_results(&mut self) {
        assert!(self.scheduler_api_client.is_some());

        let mut client = self.scheduler_api_client.as_mut().unwrap();

        let keys: Vec<u64> = self.jobs.keys().cloned().collect();
        for query_id in keys {
            let job = self.jobs.get(&query_id).unwrap_or_else(|| {
                panic!("poll_results: job for query id {} does not exist", query_id)
            });

            // Send request to scheduler only when job is in progress
            if job.status != InProgress {
                continue;
            }

            self.num_running_jobs -= 1;
            self.num_finished_jobs += 1;

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

                    let v = self.jobs.insert(query_id, updated_job_info);
                    // original value must already exist in the map
                    assert!(v.is_some());
                }

                QueryStatus::NotFound => {
                    panic!(
                        "poll_results: inconsistent state: query id {} not found from the scheduler",
                        query_id
                    );
                }

                QueryStatus::Failed => {
                    let updated_job_info = JobInfo {
                        status: QueryStatus::Failed,
                        finished_at: Some(time::Instant::now()),
                        result: None,
                        submitted_at: job.submitted_at,
                        sql_string: job.sql_string.to_string(),
                    };
                    let v = self.jobs.insert(query_id, updated_job_info);
                    // original value must already exist in the map
                    assert!(!v.is_none());
                }
                _ => {
                    panic!("poll_results: invalid query status: {:?}", new_query_status);
                }
            }
        }
    }
}
