use lazy_static::lazy_static;
use prost::Message;
use tonic::{transport::Server, Request, Response, Status};

use composable_database::scheduler_api_server::{SchedulerApi, SchedulerApiServer};
use composable_database::{
    AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
    QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
    TaskId,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once};

use crate::parser::deserialize_physical_plan;
use crate::scheduler::Scheduler;

// Static query_id generator
static QID_COUNTER: AtomicU64 = AtomicU64::new(0);
static QID_COUNTER_INIT: Once = Once::new();

// TODO: find better way to do this
lazy_static! {
    static ref SCHEDULER: Mutex<Scheduler> = Mutex::new(Scheduler::new());
}

fn next_query_id() -> u64 {
    QID_COUNTER_INIT.call_once(|| {});
    QID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

pub mod composable_database {
    tonic::include_proto!("composable_database");
}

#[derive(Debug, Default)]
pub struct SchedulerService {}

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
            let plan = deserialize_physical_plan(physical_plan.as_slice())
                .await
                .unwrap();
            let qid = next_query_id();
            // Generate query graph and schedule
            SCHEDULER.lock().unwrap().schedule_plan(qid, plan);
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
            result,
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
        let mut scheduler = SCHEDULER.lock().unwrap();
        scheduler.store_result(result);
        scheduler.update_task_state(task_id.query_id, task_id.task_id);
        if let Ok((task, bytes)) = scheduler.next_task() {
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
