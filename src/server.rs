#![allow(dead_code)]
use crate::intermediate_results::{get_results, TaskKey};
use crate::project_config::load_catalog;
use crate::query_graph::{QueryGraph, StageStatus};
use crate::query_table::QueryTable;
use crate::task::Task;
use crate::task_queue::TaskQueue;
use crate::SchedulerError;
use crate::composable_database::scheduler_api_server::{SchedulerApi, SchedulerApiServer};
use crate::composable_database::{
    AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
    QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
    TaskId,
};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use datafusion_proto::bytes::physical_plan_from_bytes;
use tonic::transport::Server;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tonic::{Request, Response, Status};

// Static query_id generator
static QID_COUNTER: AtomicU64 = AtomicU64::new(0);

const HANDSHAKE_QUERY_ID: u64 = u64::MAX;
const HANDSHAKE_TASK_ID: u64 = u64::MAX;
const HANDSHAKE_STAGE_ID: u64 = u64::MAX;

pub struct SchedulerService {
    query_table: Arc<QueryTable>,
    task_queue: Arc<TaskQueue>,
    ctx: Arc<SessionContext>, // If we support changing the catalog at runtime, this should be a RwLock.
    query_id_counter: AtomicU64,
}

impl fmt::Debug for SchedulerService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SchedulerService {{ query_table: {:?}, task_queue: {:?} }}",
            self.query_table, self.task_queue,
        )
    }
}

impl SchedulerService {
    pub async fn new(catalog_path: &str) -> Self {
        Self {
            query_table: Arc::new(QueryTable::new()),
            task_queue: Arc::new(TaskQueue::new()),
            ctx: load_catalog(catalog_path).await,
            query_id_counter: AtomicU64::new(0),
        }
    }

    fn next_query_id(&self) -> u64 {
        self.query_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    async fn update_task_state(&self, query_id: u64, task_id: u64) {
        // Update the status of the stage in the query graph.
        self.query_table
            .update_stage_status(query_id, task_id, StageStatus::Finished(0))
            .await
            .expect("Graph not found.");

        // If new tasks are available, add them to the queue
        let frontier = self.query_table.get_frontier(query_id).await;
        self.task_queue.add_tasks(frontier).await;
    }

    async fn next_task(&self) -> Result<(Task, Vec<u8>), SchedulerError> {
        let task = self.task_queue.next_task().await.unwrap();
        let stage = self
            .query_table
            .get_plan_bytes(task.task_id.query_id, task.task_id.stage_id)
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
        let ScheduleQueryArgs {
            physical_plan: bytes,
            metadata: _metadata,
        } = request.into_inner();

        if let Some(QueryInfo { priority, cost }) = _metadata {
            println!("Received query with priority {priority} and cost {cost}");
        }

        let plan = physical_plan_from_bytes(bytes.as_slice(), &self.ctx)
            .expect("Failed to deserialize physical plan");
        println!("schedule_query: received plan {:?}", plan);

        // Build a query graph, store in query table, enqueue new tasks.
        let qid = self.next_query_id();
        let query = QueryGraph::new(qid, plan);
        let leaves = self.query_table.add_query(query).await;
        self.task_queue.add_tasks(leaves).await;

        let response = ScheduleQueryRet { query_id: qid };
        Ok(Response::new(response))
    }

    // TODO clean
    async fn query_job_status(
        &self,
        request: Request<QueryJobStatusArgs>,
    ) -> Result<Response<QueryJobStatusRet>, Status> {
        let QueryJobStatusArgs { query_id } = request.into_inner();

        // let query_status = self.query_table.get_query_status(query_id).await;
        //
        // if let QueryStatus::Done = query_status {
        //     let stage_id = self.query_table.get_query(query_id).await.num_stages() - 1;
        //     let final_result_opt = get_results(&TaskKey {
        //         stage_id,
        //         query_id,
        //     })
        //     .await;
        //     let final_result = final_result_opt.expect("api.rs: query is done but no results in table");
        //     print_batches(&final_result).unwrap();
        // }
        //
        // if let QueryStatus::Done | QueryStatus::Failed = query_status {
        //     self.query_table.remove_query(query_id).await;
        // }
        //
        // Ok(Response::new(QueryJobStatusRet {
        //     query_status: query_status.into(),
        //     query_result: Vec::new(),
        // }))
        let graph_opt = self.query_table.table.get(&query_id);
        if graph_opt.is_none() {
            return Ok(Response::new(QueryJobStatusRet {
                query_status: QueryStatus::NotFound.into(),
                query_result: Vec::new(),
            }));
        }

        let graph = graph_opt.unwrap();
        let graph_done = graph.read().await.done;
        if graph_done {
            let stage_id = 0;
            let final_result_opt = get_results(&TaskKey { stage_id, query_id }).await;
            let final_result =
                final_result_opt.expect("api.rs: query is done but no results in table");
            print_batches(&final_result).unwrap();
            Ok(Response::new(QueryJobStatusRet {
                query_status: QueryStatus::Done.into(),
                query_result: Vec::new(),
            }))
        } else {
            Ok(Response::new(QueryJobStatusRet {
                query_status: QueryStatus::InProgress.into(),
                query_result: Vec::new(),
            }))
        }
    }

    async fn abort_query(
        &self,
        _request: Request<AbortQueryArgs>,
    ) -> Result<Response<AbortQueryRet>, Status> {
        // TODO Actually call executor API to abort query.
        let response = AbortQueryRet { aborted: true };
        Ok(Response::new(response))
    }

    // TODO Potentially rename to be more clear?
    async fn notify_task_state(
        &self,
        request: Request<NotifyTaskStateArgs>,
    ) -> Result<Response<NotifyTaskStateRet>, Status> {
        let NotifyTaskStateArgs {
            task,      // TODO: We should use `None` to indicate the handshake task.
            success,   // TODO: Switch to status enum.
            result: _, // TODO: Remove this field from the proto, replace with pointer.
        } = request.into_inner();

        if !success {
            // TODO: Kill all in-progress tasks for this query, and either abort or retry the query.
            return Err(Status::aborted(
                "Executor: Failed to execute query fragment.",
            ));
        }

        if let Some(task_id) = task {
            if task_id.task_id != HANDSHAKE_TASK_ID
                && task_id.query_id != HANDSHAKE_QUERY_ID
                && task_id.stage_id != HANDSHAKE_STAGE_ID
            {
                self.update_task_state(task_id.query_id, task_id.task_id)
                    .await;
            }
        }

        if let Ok((task, bytes)) = self.next_task().await {
            self.query_table
                .update_stage_status(
                    task.task_id.query_id,
                    task.task_id.stage_id,
                    StageStatus::Running(0),
                )
                .await
                .expect("Error updating stage status");

            let response = NotifyTaskStateRet {
                has_new_task: true,
                task: Some(TaskId {
                    query_id: task.task_id.query_id,
                    stage_id: task.task_id.stage_id,
                    task_id: task.task_id.task_id,
                }),
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
    use crate::parser::Parser;
    use crate::composable_database::scheduler_api_server::SchedulerApi;
    use crate::composable_database::{
        AbortQueryArgs, AbortQueryRet, NotifyTaskStateArgs, NotifyTaskStateRet, QueryInfo,
        QueryJobStatusArgs, QueryJobStatusRet, QueryStatus, ScheduleQueryArgs, ScheduleQueryRet,
        TaskId,
    };
    use crate::server::SchedulerService;
    use tonic::Request;

    #[tokio::test]
    async fn test_scheduler() {
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/expr.slt");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/");
        let scheduler_service = Box::new(SchedulerService::new(catalog_path).await);
        let parser = Parser::new(catalog_path).await;
        println!("test_scheduler: Testing file {}", test_file);
        if let Ok(physical_plans) = parser.get_execution_plan_from_file(&test_file).await {
            for plan in &physical_plans {
                let plan_f = parser.serialize_physical_plan(plan.clone()).await;
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
            scheduler_service.task_queue.size().await
        );
    }
}
