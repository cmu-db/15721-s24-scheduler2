use prost::Message;
use tonic::{transport::Server, Request, Response, Status};

use composable_database::scheduler_server::{Scheduler, SchedulerServer};
use composable_database::{
    AbortQueryArgs, AbortQueryRet, NewTaskPlan, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo, QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet
};
// use crate::scheduler::Scheduler;

use substrait::proto::Plan;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

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

#[derive(Debug, Default)]
pub struct SchedulerService {
    // scheduler: scheduler::Scheduler,
}

#[tonic::async_trait]
impl Scheduler for SchedulerService {
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
            let _plan = Plan::decode(physical_plan.as_slice()).unwrap();
        } else {
            let _ = Err::<Response<ScheduleQueryRet>, Status>(Status::invalid_argument(
                "Missing metadata in request",
            ));
        }
        let qid = next_query_id();
        // Generate query graph and schedule
        let response = ScheduleQueryRet { query_id: qid };
        Ok(Response::new(response))
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
        if let NotifyTaskStateArgs {
            task_id,
            state
        } = request.into_inner()
        {
        } else {
            let _ = Err::<Response<NotifyTaskStateRet>, Status>(Status::invalid_argument(
                "Missing metadata in request",
            ));
        }
        let response = NewTaskPlan::new();
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = env!("CARGO_MANIFEST_DIR");
    println!("Path to crate's root: {}", crate_root);
    let addr = "[::1]:50051".parse()?;
    let scheduler_service = SchedulerService::default();
    Server::builder()
        .add_service(SchedulerServer::new(scheduler_service))
        .serve(addr)
        .await?;
    Ok(())
}
