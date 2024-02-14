use tonic::{transport::Server, Request, Response, Status};
use prost::Message;

use composable_database::scheduler_server::{Scheduler, SchedulerServer};
use composable_database::{ScheduleQueryArgs, ScheduleQueryRet, QueryInfo, QueryStatus,
    QueryJobStatusArgs, QueryJobStatusRet, AbortQueryArgs, AbortQueryRet};

use substrait::proto::Plan;

pub mod composable_database {
    tonic::include_proto!("composable_database");
}

#[derive(Debug, Default)]
pub struct SchedulerService {}

#[tonic::async_trait]
impl Scheduler for SchedulerService {
    async fn schedule_query(
        &self,
        request: Request<ScheduleQueryArgs>,
    ) -> Result<Response<ScheduleQueryRet>, Status> {


        if let ScheduleQueryArgs {
            physical_plan,
            metadata: Some(QueryInfo {priority, cost})
        } = request.into_inner()
        {
            println!("Got a request with priority {:?} and cost {:?}", priority, cost);
            let _plan = Plan::decode(physical_plan.as_slice()).unwrap();
        }
        else {
            let _ = Err::<Response<ScheduleQueryRet>, Status>(Status::invalid_argument("Missing metadata in request"));
        }

        let response = ScheduleQueryRet { query_id: 0 };
        Ok(Response::new(response))
    }

    async fn query_job_status(&self,
        _request: Request<QueryJobStatusArgs>,
    ) -> Result<Response<QueryJobStatusRet>, Status> {
        let response = QueryJobStatusRet { query_status: QueryStatus::Done.into()};
        Ok(Response::new(response))
    }

    async fn abort_query(&self,
        _request: Request<AbortQueryArgs>,
    ) -> Result<Response<AbortQueryRet>, Status> {
        let response = AbortQueryRet { aborted: true };
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
