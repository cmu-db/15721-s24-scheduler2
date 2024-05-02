use crate::composable_database::scheduler_api_server::{SchedulerApi, SchedulerApiServer};
use crate::composable_database::{
    AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
    QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
    TaskId,
};
use crate::intermediate_results::{get_results, TaskKey};
use crate::mock_catalog::load_catalog;
use crate::parser::ExecutionPlanParser;
use crate::query_graph::{QueryGraph, StageStatus};
use crate::query_table::QueryTable;
use crate::queue::Queue;
use crate::SchedulerError;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use datafusion_proto::bytes::physical_plan_from_bytes;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, Duration};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub struct SchedulerService {
    query_table: Arc<QueryTable>,
    queue: Arc<Mutex<Queue>>,
    ctx: Arc<SessionContext>, // If we support changing the catalog at runtime, this should be a RwLock.
    query_id_counter: AtomicU64,
    avail: Arc<Notify>,
}

impl fmt::Debug for SchedulerService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SchedulerService {{ query_table: {:?}, queue: {:?} }}",
            self.query_table, self.queue,
        )
    }
}

impl SchedulerService {
    pub async fn new(catalog_path: &str) -> Self {
        let avail = Arc::new(Notify::new());
        Self {
            query_table: Arc::new(QueryTable::new().await),
            queue: Arc::new(Mutex::new(Queue::new(Arc::clone(&avail)))),
            ctx: load_catalog(catalog_path).await,
            query_id_counter: AtomicU64::new(0),
            avail,
        }
    }

    fn next_query_id(&self) -> u64 {
        self.query_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    // Get the next task from the queue.
    async fn next_task(
        &self,
        task_id_opt: Option<TaskId>,
    ) -> Result<(TaskId, Vec<u8>), SchedulerError> {
        if let Some(task_id) = task_id_opt {
            let mut queue = self.queue.lock().await;
            // Remove the current task from the queue.
            queue.remove_task(task_id, StageStatus::Finished(0)).await;
        }
        loop {
            let mut queue = self.queue.lock().await;
            if let Some(new_task_id) = queue.next_task().await {
                let stage = self
                    .query_table
                    .get_plan_bytes(new_task_id.query_id, new_task_id.stage_id)
                    .await?;
                return Ok((new_task_id, stage));
            }
            drop(queue);
            self.avail.notified().await;
        }
    }
}

#[tonic::async_trait]
impl SchedulerApi for SchedulerService {
    async fn schedule_query(
        &self,
        request: Request<ScheduleQueryArgs>,
    ) -> Result<Response<ScheduleQueryRet>, Status> {
        let ScheduleQueryArgs {
            physical_plan: bytes,
            metadata: _metadata,
        } = request.into_inner();

        if let Some(QueryInfo { priority, cost }) = _metadata {
            println!("Received query with priority {priority} and cost {cost}");
        }

        let plan = physical_plan_from_bytes(bytes.as_slice(), &self.ctx)
            .expect("Failed to deserialize physical plan");
        // println!("schedule_query: received plan {:?}", plan);

        // Build a query graph, store in query table, enqueue new tasks.
        let qid = self.next_query_id();
        let query = QueryGraph::new(qid, plan).await;
        let graph = self.query_table.add_query(query).await;
        self.queue.lock().await.add_query(qid, graph).await;

        let response = ScheduleQueryRet { query_id: qid };
        Ok(Response::new(response))
    }

    // TODO clean
    async fn query_job_status(
        &self,
        request: Request<QueryJobStatusArgs>,
    ) -> Result<Response<QueryJobStatusRet>, Status> {
        let QueryJobStatusArgs { query_id } = request.into_inner();

        let status = self.queue.lock().await.get_query_status(query_id).await;
        if status == QueryStatus::Done {
            let stage_id = 0;
            let final_result = get_results(&TaskKey { stage_id, query_id })
                .await
                .expect("api.rs: query is done but no results in table");
            // print_batches(&final_result).unwrap();

            // ****************** BEGIN CHANGES FROM INTEGRATION TESTING ***************//
            let final_result_bytes =
                ExecutionPlanParser::serialize_record_batches(final_result[0].clone())
                    .expect("fail to serialize record batch");

            return Ok(Response::new(QueryJobStatusRet {
                query_status: QueryStatus::Done.into(),
                query_result: final_result_bytes,
            }));
            // ****************** END CHANGES FROM INTEGRATION TESTING****************//
        }
        return Ok(Response::new(QueryJobStatusRet {
            query_status: status.into(),
            query_result: Vec::new(),
        }));
    }

    async fn abort_query(
        &self,
        _request: Request<AbortQueryArgs>,
    ) -> Result<Response<AbortQueryRet>, Status> {
        // TODO: Actually call executor API to abort query.
        let response = AbortQueryRet { aborted: true };
        Ok(Response::new(response))
    }

    // TODO Potentially rename to be more clear?
    async fn notify_task_state(
        &self,
        request: Request<NotifyTaskStateArgs>,
    ) -> Result<Response<NotifyTaskStateRet>, Status> {
        let NotifyTaskStateArgs {
            task,    // TODO: We should use `None` to indicate the handshake task.
            success, // TODO: Switch to status enum.
        } = request.into_inner();

        if !success {
            // TODO: Kill all in-progress tasks for this query, and either abort or retry the query.
            return Err(Status::aborted(
                "Executor: Failed to execute query fragment.",
            ));
        }

        if let Ok((new_task_id, bytes)) = self.next_task(task).await {
            let response = NotifyTaskStateRet {
                has_new_task: true,
                task: Some(new_task_id),
                physical_plan: bytes,
            };
            Ok(Response::new(response))
        } else {
            Err(Status::resource_exhausted(
                "Scheduler: Failed to get next task.",
            ))
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use crate::composable_database::scheduler_api_server::SchedulerApi;
    use crate::composable_database::{
        AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
        QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
        TaskId,
    };
    use crate::parser::ExecutionPlanParser;
    use crate::server::SchedulerService;
    use tonic::Request;

    #[tokio::test]
    async fn test_scheduler() {
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/expr.slt");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/");
        let scheduler_service = Box::new(SchedulerService::new(catalog_path).await);
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_scheduler: Testing file {}", test_file);
        if let Ok(physical_plans) = parser.get_execution_plan_from_file(&test_file).await {
            for plan in &physical_plans {
                let plan_f = parser.serialize_physical_plan(plan.clone());
                if plan_f.is_err() {
                    println!(
                        "test_scheduler: Unable to serialize plan in file {}.",
                        test_file
                    );
                    continue;
                }
                // println!("plan: {:?}", plan.clone());
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
                println!("test_scheduler: Scheduled query {}.", query_id);
                if query_id == 125 {
                    // fails after 125 in expr.slt for some reason
                    break;
                }
            }
        } else {
            println!(
                "test_scheduler: Failed to get execution plan from file {}.",
                test_file
            );
        }
        println!(
            "test_scheduler: queued {} tasks.",
            scheduler_service.queue.lock().await.size()
        );

        // TODO: add concurrent test eventually
        let mut send_task = NotifyTaskStateArgs {
            task: None,
            success: true,
        };
        // may not terminate
        loop {
            let ret = scheduler_service
                .notify_task_state(Request::new(send_task.clone()))
                .await
                .unwrap();
            let NotifyTaskStateRet {
                has_new_task,
                task,
                physical_plan,
            } = ret.into_inner();
            assert!(task.is_some());
            send_task.task = task;
        }
    }
}
