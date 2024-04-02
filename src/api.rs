use tonic::{Request, Response, Status};

use composable_database::scheduler_api_server::SchedulerApi;
use composable_database::{
    AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
    QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
    TaskId,
};

use crate::query_graph::{QueryGraph, StageStatus};
use crate::query_table::QueryTable;
use crate::task::Task;
use crate::task_queue::TaskQueue;
use crate::SchedulerError;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use crate::parser::deserialize_physical_plan;

// Static query_id generator
static QID_COUNTER: AtomicU64 = AtomicU64::new(0);
static QID_COUNTER_INIT: Once = Once::new();

fn next_query_id() -> u64 {
    QID_COUNTER_INIT.call_once(|| {});
    QID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

pub mod composable_database {
    tonic::include_proto!("composable_database");
}

#[derive(Debug)]
pub struct SchedulerService {
    // Internal state of queries.
    query_table: QueryTable,

    // Task queue
    task_queue: TaskQueue,
}

impl SchedulerService {
    pub fn new() -> Self {
        Self {
            query_table: QueryTable::new(),
            task_queue: TaskQueue::new(),
        }
    }

    async fn update_task_state(&self, query_id: u64, task_id: u64) {
        // Update the status of the stage in the query graph.
        self.query_table
            .update_stage_status(query_id, task_id, StageStatus::Finished(0))
            .await
            .expect("Graph not found.");

        // If new tasks are available, add them to the queue
        let frontier = self.query_table.get_frontier(query_id).await;
        self.task_queue.add_tasks(frontier);
    }

    async fn next_task(&self) -> Result<(Task, Vec<u8>), SchedulerError> {
        let task = self.task_queue.next_task();
        let stage = self
            .query_table
            .get_plan_bytes(task.query_id, task.stage_id)
            .await?;
        Ok((task, stage))
    }
}

#[tonic::async_trait]
impl SchedulerApi for SchedulerService {
    async fn schedule_query(
        &self,
        request: Request<ScheduleQueryArgs>,
    ) -> Result<Response<ScheduleQueryRet>, Status> {
        if let ScheduleQueryArgs {
            physical_plan,
            metadata: Some(QueryInfo { priority, cost }),
        } = request.into_inner()
        {
            println!(
                "Got a request with priority {:?} and cost {:?}",
                priority, cost
            );
            let plan = deserialize_physical_plan(physical_plan.as_slice().to_vec())
                .await
                .unwrap();
            println!("schedule_query: received plan {:?}", plan);
            let qid = next_query_id();

            // Generate query graph and schedule
            // Build a query graph from the plan.
            let query = QueryGraph::new(qid, plan);
            let frontier = self.query_table.add_query(query).await;

            // Add the query to the task queue.
            self.task_queue.add_tasks(frontier);

            let response = ScheduleQueryRet { query_id: qid };
            return Ok(Response::new(response));
        } else {
            return Err::<Response<ScheduleQueryRet>, Status>(Status::invalid_argument(
                "Missing metadata in request",
            ));
        }
    }

    async fn query_job_status(
        &self,
        _request: Request<QueryJobStatusArgs>,
    ) -> Result<Response<QueryJobStatusRet>, Status> {
        // Get actual status from queryID table
        let response = QueryJobStatusRet {
            query_status: QueryStatus::Done.into(),
        };
        Ok(Response::new(response))
    }

    async fn abort_query(
        &self,
        _request: Request<AbortQueryArgs>,
    ) -> Result<Response<AbortQueryRet>, Status> {
        // handle response
        let response = AbortQueryRet { aborted: true };
        Ok(Response::new(response))
    }

    async fn notify_task_state(
        &self,
        request: Request<NotifyTaskStateArgs>,
    ) -> Result<Response<NotifyTaskStateRet>, Status> {
        let NotifyTaskStateArgs {
            task,
            success,
            result: _,
        } = request.into_inner();

        let task_id = match task {
            Some(t) => t,
            None => {
                return Err(Status::invalid_argument(
                    "Executor: Failed to match task ID.",
                ))
            }
        };
        if !success {
            // send kill message to all executors
            // Flag query as aborted (for now)
            // Another option is to requeue failed fragment
            return Err(Status::invalid_argument(
                "Executor: Failed to execute query fragment.",
            ));
        }
        self.update_task_state(task_id.query_id, task_id.task_id)
            .await;
        if let Ok((task, bytes)) = self.next_task().await {
            let response = NotifyTaskStateRet {
                has_new_task: true,
                task: Some(TaskId {
                    query_id: task.query_id,
                    task_id: task.id,
                }),
                physical_plan: bytes,
            };
            return Ok(Response::new(response));
        } else {
            return Err(Status::invalid_argument(
                "Scheduler: Failed to get next task.",
            ));
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use crate::api::composable_database::scheduler_api_server::SchedulerApi;
    use crate::api::composable_database::{
        AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
        QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
        TaskId,
    };
    use crate::api::SchedulerService;
    use crate::parser::{
        deserialize_physical_plan, get_execution_plan_from_file, serialize_physical_plan,
    };
    use tonic::Request;

    #[tokio::test]
    async fn test_scheduler() {
        let scheduler_service = Box::new(SchedulerService::new());
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/expr.slt");
        println!("test_scheduler: Testing file {}", test_file);
        if let Ok(physical_plans) = get_execution_plan_from_file(&test_file).await {
            for plan in &physical_plans {
                let plan_f = serialize_physical_plan(plan.clone()).await;
                if plan_f.is_err() {
                    println!(
                        "test_scheduler: Unable to serialize plan in file {}.",
                        test_file
                    );
                    continue;
                }
                println!("plan: {:?}", plan.clone());
                let plan_bytes: Vec<u8> = plan_f.unwrap();
                println!("Serialized plan is {} bytes.", plan_bytes.len());
                let query = ScheduleQueryArgs {
                    physical_plan: plan_bytes,
                    metadata: Some(QueryInfo {
                        priority: 0,
                        cost: 0,
                    }),
                };
                let response = scheduler_service.schedule_query(Request::new(query)).await;
                if response.is_err() {
                    println!(
                        "test_scheduler: schedule_query failed in file {}.",
                        test_file
                    );
                    continue;
                }
                let query_id = response.unwrap().into_inner().query_id;
                println!("test_scheduler: Queued query {}.", query_id);
                if query_id == 125 {
                    return;
                }
            }
        } else {
            println!(
                "test_scheduler: Failed to get execution plan from file {}.",
                test_file
            );
        }
        println!("test_scheduler: ok.");
    }
}