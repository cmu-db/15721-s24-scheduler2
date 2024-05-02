use crate::composable_database::{QueryStatus, TaskId};
use crate::query_graph::{QueryGraph, QueryQueueStatus, StageStatus};
use crate::task::{
    Task,
    TaskStatus::{self, *},
};
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{Mutex, Notify, RwLock};

// Must implement here since generated TaskId does not derive Hash.
impl Hash for TaskId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // You can choose which fields to include in the hash here
        // For example, if MyMessage has fields `id` and `name`, you can hash them like this:
        self.task_id.hash(state);
        self.stage_id.hash(state);
        self.query_id.hash(state);
    }
}

impl Eq for TaskId {}

impl Copy for TaskId {}

#[derive(Debug)]
pub struct State {
    // The queue used to order queries by executor usage.
    // queue: Mutex<BTreeMap<Duration, u64>>,
    queue: Mutex<VecDeque<u64>>,
    start_ts: SystemTime,

    query_id_counter: AtomicU64,
    // Structure that maps query IDs to query keys.
    table: DashMap<u64, RwLock<QueryGraph>>,
    // List of currently running tasks.
    running_tasks: DashMap<TaskId, Task>,
    // Notify primitive that signals when new tasks are ready.
    notify: Arc<Notify>,
}

impl State {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            // queue: Mutex::new(BTreeMap::new()),
            queue: Mutex::new(VecDeque::new()),
            start_ts: SystemTime::now(),
            query_id_counter: AtomicU64::new(0),
            table: DashMap::new(),
            running_tasks: DashMap::new(),
            notify,
        }
    }

    fn next_query_id(&self) -> u64 {
        self.query_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn add_query(&self, plan: Arc<dyn ExecutionPlan>) -> u64 {
        let id = self.next_query_id();
        let mut query = QueryGraph::new(id, plan);
        let time = SystemTime::now().duration_since(self.start_ts).unwrap();
        query.time = time;

        self.table.insert(id, RwLock::new(query));
        // self.queue.lock().await.insert(time, id);
        self.queue.lock().await.push_back(id);

        self.notify.notify_waiters();
        id
    }

    pub async fn get_query_status(&self, query_id: u64) -> Option<QueryStatus> {
        let status = self.table.get(&query_id)?.read().await.status;
        if status == QueryStatus::Done {
            self.table.remove(&query_id);
        }
        Some(status)
    }

    // TODO: Graceful abort.
    pub async fn abort_query(&self, query_id: u64) {
        if let Some(query) = self.table.get(&query_id) {
            {
                let mut guard = query.write().await;
                guard.abort();
            }
            self.table.remove(&query_id);
        }
    }

    pub async fn next_task(&self) -> Option<(TaskId, Arc<dyn ExecutionPlan>)> {
        // let Some((duration, query_id)) = self.queue.lock().await.pop_first() else {
        let Some(query_id) = self.queue.lock().await.pop_front() else {
            return None;
        };
        let query = self.table.get(&query_id).unwrap();
        let mut guard = query.write().await;

        let mut task = guard.next_task();
        task.status = Running(SystemTime::now());
        let task_id = task.task_id;
        let plan = guard.get_plan(task.task_id.stage_id);

        // Update query to reflect running task. Requeue if more tasks are available.
        guard
            .update_stage_status(task.task_id.stage_id, StageStatus::Running(0))
            .unwrap();
        if let QueryQueueStatus::Available = guard.get_queue_status() {
            // self.queue.lock().await.insert(duration, query_id);
            self.queue.lock().await.push_back(query_id);
            self.notify.notify_waiters();
        }

        self.running_tasks.insert(task_id, task);
        Some((task_id, plan))
    }

    pub async fn report_task(&self, task_id: TaskId, status: TaskStatus) {
        if let Some((_, task)) = self.running_tasks.remove(&task_id) {
            println!("Updating {:?} status to {:?}", task_id, status);
            let TaskStatus::Running(_ts) = task.status else {
                println!("Task removed with status {:?}", task.status);
                panic!("Task removed but is not running.");
            };
            let query = self.table.get(&task_id.query_id).unwrap();
            let mut guard = query.write().await;

            match status {
                TaskStatus::Finished => guard
                    .update_stage_status(task_id.stage_id, StageStatus::Finished(0))
                    .unwrap(),
                TaskStatus::Failed => todo!(),
                TaskStatus::Aborted => todo!(),
                _ => unreachable!(),
            }

            let mut queue = self.queue.lock().await;
            // let new_time = guard.time + SystemTime::now().duration_since(ts).unwrap();
            // let _ = queue.remove(&guard.time);
            // if let QueryQueueStatus::Available = guard.get_queue_status() {
            //     queue.insert(new_time, task_id.query_id);
            //     self.notify.notify_waiters();
            // }
            // guard.time = new_time;
            if QueryQueueStatus::Available == guard.get_queue_status()
                && !queue.contains(&task_id.query_id)
            {
                queue.push_back(task_id.query_id);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn size(&self) -> usize {
        self.queue.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::{fs, time::{Duration, SystemTime}};
    use tokio::{sync::Notify, time::sleep};

    use crate::queue::State;
    use crate::task::TaskStatus;
    use crate::{parser::ExecutionPlanParser, query_graph::QueryGraph};
    use std::{cmp::min, sync::Arc};

    // Deprecated, use test_queue_conc instead
    #[tokio::test]
    async fn test_queue() {
        let test_file = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_sql/test_select_multiple.sql"
        );
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/");
        let queue = Box::new(State::new(Arc::new(Notify::new())));
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_scheduler: Testing file {}", test_file);
        if let Ok(physical_plans) = parser.get_execution_plan_from_file(&test_file).await {
            let mut qid = 0;
            // Add a bunch of queries
            for plan in &physical_plans {
                queue.add_query(Arc::clone(plan)).await;
                qid += 1;
            }
            let mut tasks = Vec::new();
            for _ in 0..qid {
                tasks.push(queue.next_task().await.expect("No tasks left in queue."));
            }
            for _ in 0..qid {
                queue
                    .report_task(tasks.pop().unwrap().0, TaskStatus::Finished)
                    .await;
            }
        } else {
            panic!("Plan was not parsed.");
        }
    }

    #[tokio::test]
    async fn test_queue_conc() {
        let test_sql_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql/");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/");
        let queue = Arc::new(State::new(Arc::new(Notify::new())));
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_queue_conc: Testing files in {}", test_sql_dir);

        let mut physical_plans = Vec::new();
        for file in fs::read_dir(test_sql_dir).unwrap() {
            let path_buf = file.unwrap().path();
            let path = path_buf.to_str().unwrap();
            physical_plans.extend(parser.get_execution_plan_from_file(&path).await.unwrap());
        }

        println!("Got {} plans.", physical_plans.len());
        let nplans = min(physical_plans.len(), 400);
        let plans = physical_plans[..nplans].to_vec();

        // Add a bunch of queries
        let mut jobs = Vec::new();
        for plan in plans {
            let queue_clone = Arc::clone(&queue);
            // Spawn threads that each enqueue a job
            jobs.push(tokio::spawn(async move {
                let _ = queue_clone.add_query(Arc::clone(&plan)).await;
            }));
        }

        // spawn as many tasks as there are queries.
        // WARNING: may need more as pipelines are added.
        for _ in 0..nplans {
            let queue_clone = Arc::clone(&queue);
            jobs.push(tokio::spawn(async move {
                // Get a plan, looping until one is available
                loop {
                    let task_opt = queue_clone.next_task().await;
                    if let Some((task, _plan)) = task_opt {
                        queue_clone.report_task(task, TaskStatus::Finished).await;
                    }
                    let time = rand::thread_rng().gen_range(200..4000);
                    tokio::time::sleep(tokio::time::Duration::from_millis(time)).await;
                    // Return if no more queries left.
                    if queue_clone.size().await == 0 {
                        return;
                    }
                }
            }));
        }

        println!("Waiting for {} jobs.", &jobs.len());
        for job in jobs {
            let _ = job.await.unwrap();
        }
        // println!("Queued {} queries.", qid.lock().await.load(Ordering::SeqCst));
        // make sure no more tasks remain
        assert!(Arc::clone(&queue).next_task().await.is_none());
        assert!(queue.size().await == 0);
        println!("Finished {:?} tasks.", nplans);
    }

    // Test correctness of stride scheduling algorithm.
    #[tokio::test]
    async fn test_stride() {
        let test_sql_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql/");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/");
        let queue = Box::new(State::new(Arc::new(Notify::new())));
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_queue_conc: Testing files in {}", test_sql_dir);

        // Generate list of all tpch plans
        let mut physical_plans = Vec::new();
        for file in fs::read_dir(test_sql_dir).unwrap() {
            let path_buf = file.unwrap().path();
            let path = path_buf.to_str().unwrap();
            physical_plans.extend(parser.get_execution_plan_from_file(&path).await.unwrap());
        }

        println!("Got {} plans.", physical_plans.len());
        let mut long_plans = Vec::new();

        // Generate list of plans with at least `rounds` stages
        let rounds = 5;
        for plan in physical_plans {
            let graph = QueryGraph::new(0, Arc::clone(&plan));
            if graph.stages.len() >= rounds {
                long_plans.push(plan);
            }
        }
        let nplans = long_plans.len();

        // Add a bunch of queries with staggered submission time
        let start_enqueue = SystemTime::now();
        for plan in long_plans {
            queue.add_query(Arc::clone(&plan)).await;
            sleep(Duration::from_millis(10)).await;
        }
        let enq_time = SystemTime::now().duration_since(start_enqueue).unwrap();

        // Ensure correct order of queue
        for rnd in 0..rounds {
            for i in 0..nplans {
                let (task, _) = queue.next_task().await.unwrap();
                // Queries should be processed in order
                assert_eq!(task.query_id, i as u64);
                // "process" for at least as long as (max_pass - min_pass)
                sleep(enq_time).await;
                // Return task; update query's pass
                queue.report_task(task, TaskStatus::Finished).await;
                println!("(Round {}) Query {}/{} ok.", rnd + 1, i + 1, nplans);
            }
        }
    }
}
