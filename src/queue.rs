use crate::composable_database::{QueryStatus, TaskId};
use crate::query_graph::{QueryGraph, QueryQueueStatus, StageStatus};
use crate::task::{
    Task,
    TaskStatus::{self, *},
};
use crate::SchedulerError;
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::physical_plan_to_bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct QueryKey {
    pub ft: Duration,
    pub qid: u64,
}

#[derive(Debug)]
pub struct Queue {
    // The queue used to order queries by executor usage.
    queue: BTreeSet<QueryKey>,
    // The startup time of the queue, used to calculate new global passes.
    start_ts: SystemTime,
    // Structure that maps query IDs to query keys.
    query_map: HashMap<u64, (Arc<Mutex<QueryKey>>, Arc<Mutex<QueryGraph>>)>,
    // table: DashMap<u64, RwLock<QueryGraph>>,
    // List of currently running tasks.
    running_task_map: HashMap<TaskId, Task>,
    // Notify primitive that signals when new tasks are ready.
    avail: Arc<Notify>,
}

pub struct State {
    // queue: Mutex<VecDeque<QueryKey>>,
    queue: Mutex<BTreeMap<Duration, u64>>,
    start_ts: SystemTime,

    query_id_counter: AtomicU64,
    table: DashMap<u64, RwLock<QueryGraph>>,
    running_tasks: DashMap<TaskId, Task>,
    notify: Arc<Notify>,
}

impl State {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            queue: Mutex::new(BTreeMap::new()),
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
        self.queue.lock().await.insert(time, id);

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

    pub async fn abort_query(&self, query_id: u64) {
        todo!()
    }

    pub async fn next_task(&self) -> Option<(TaskId, Arc<dyn ExecutionPlan>)> {
        let Some((duration, query_id)) = self.queue.lock().await.pop_first() else {
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
            self.queue.lock().await.insert(duration, query_id);
            self.notify.notify_waiters();
        }

        self.running_tasks.insert(task_id, task);
        Some((task_id, plan))
    }

    pub async fn report_task(&self, task_id: TaskId, status: TaskStatus) {
        if let Some((_, task)) = self.running_tasks.remove(&task_id) {
            println!("Updating {:?} status to {:?}", task_id, status);
            let TaskStatus::Running(ts) = task.status else {
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

            let new_time = guard.time + SystemTime::now().duration_since(ts).unwrap();
            let mut queue = self.queue.lock().await;
            let _ = queue.remove(&guard.time);
            if let QueryQueueStatus::Available = guard.get_queue_status() {
                queue.insert(new_time, task_id.query_id);
                self.notify.notify_waiters();
            }
            guard.time = new_time;
        }
    }
}

// Notify variable is shared with scheduler service to control task dispatch.
impl Queue {
    pub fn new(avail: Arc<Notify>) -> Self {
        Self {
            queue: BTreeSet::new(),
            start_ts: SystemTime::now(),
            query_map: HashMap::new(),
            running_task_map: HashMap::new(),
            avail,
        }
    }

    // Send the status update for a task to its query graph structure.
    // Based on the query's availability, change its priority.
    // Used in both add_running_task and remove_task.
    async fn update_status(
        &mut self,
        old_key: QueryKey,
        new_key: QueryKey,
        finished_stage_id: u64,
        finished_stage_status: StageStatus,
    ) {
        // Get the graph for this query
        let graph = Arc::clone(&self.query_map.get(&old_key.qid).unwrap().1);
        // Temporarily remove query from queue, if present, and get its graph
        let _ = self.queue.remove(&old_key);

        // If graph has more tasks available, re-insert query and notify
        let mut guard = graph.lock().await;
        guard
            .update_stage_status(finished_stage_id, finished_stage_status)
            .unwrap();
        if let QueryQueueStatus::Available = guard.get_queue_status() {
            self.queue.insert(new_key);
            self.avail.notify_waiters();
        }
    }

    // Mark this task as running.
    async fn add_running_task(&mut self, mut task: Task, key: QueryKey) {
        // Change the task's status to running.
        task.status = Running(SystemTime::now());
        // Add the task to the list of running tasks.
        self.running_task_map.insert(task.task_id, task.clone());
        // Send the update to the query graph and reorder queue.
        // WARNING: stage_status may not be 'running' if tasks and stages are not 1:1
        self.update_status(
            key.clone(),
            key,
            task.task_id.stage_id,
            StageStatus::Running(0),
        )
        .await;
    }

    /* Get the minimum element of the queue, or None if the queue is empty */
    fn min(&mut self) -> Option<QueryKey> {
        self.queue.pop_first()
    }

    #[cfg(test)]
    pub fn size(&self) -> usize {
        self.queue.len()
    }

    // TODO(makototomokiyo): make sure stride actually works
    pub async fn add_query(&mut self, qid: u64, graph: Arc<Mutex<QueryGraph>>) {
        let key = QueryKey {
            // running: 0,
            ft: SystemTime::now().duration_since(self.start_ts).unwrap(),
            qid,
        };
        self.query_map
            .insert(qid, (Arc::new(Mutex::new(key)), Arc::clone(&graph)));
        self.queue.insert(key);
        self.avail.notify_waiters();
    }

    /*
    Remove this task from the list of running tasks and mark it as done.
    This function forwards task info to the task's query graph,
    updating it if necessary.
    */
    // TODO: handle aborted queries
    pub async fn remove_task(&mut self, task_id: TaskId, finished_stage_status: StageStatus) {
        // Remove the task from the running map.
        let task = self.running_task_map.remove(&task_id).unwrap();
        debug_assert!(task.task_id == task_id);
        // Get the query ID.
        let query = task_id.query_id;
        // Get the key corresponding to the task's query.
        let mut key = self.query_map.get(&query).unwrap().0.lock().await;
        // Ensure the task is running.
        if let Running(start_ts) = task.status {
            let old_key = *key;
            // Increment the query's pass using the task's elapsed time.
            (*key).ft += SystemTime::now().duration_since(start_ts).unwrap();
            let new_key = *key;
            drop(key); // to avoid double mutable borrow
            self.update_status(old_key, new_key, task_id.stage_id, finished_stage_status)
                .await;
        } else {
            panic!("Task removed but is not running.");
        }
    }

    /*
    Return the next task, or None if the queue is empty.
    Blocking is handled in the server.
    */
    pub async fn next_task(&mut self) -> Option<TaskId> {
        if let Some(key) = self.min() {
            // If a query is available, get its next task
            let graph = &self.query_map.get(&key.qid).unwrap().1;
            println!("Queue size before getting task: {:#?}", self.queue.len());
            let new_task = graph.lock().await.next_task();

            debug_assert!(matches!(new_task.status, TaskStatus::Ready));

            self.add_running_task(new_task.clone(), key).await;
            Some(new_task.task_id)
        } else {
            None
        }
    }

    pub async fn get_query_status(&mut self, qid: u64) -> QueryStatus {
        if let Some(query_entry) = self.query_map.get(&qid) {
            let status = query_entry.1.lock().await.status;
            let key = *query_entry.0.lock().await;
            // If query is done, return DONE and delete from table
            if status == QueryStatus::Done {
                self.query_map.remove(&key.qid).expect("Query not found.");
            }
            return status;
        } else {
            return QueryStatus::NotFound;
        }
    }

    pub async fn abort_query(&mut self, qid: u64) {
        if let Some(query_entry) = self.query_map.get(&qid) {
            query_entry.1.lock().await.abort();
            self.query_map.remove(&qid);
        }
    }

    pub async fn get_plan_bytes(
        &self,
        query_id: u64,
        stage_id: u64,
    ) -> Result<Vec<u8>, SchedulerError> {
        let t = &self.query_map;
        if let Some((_, graph)) = t.get(&query_id) {
            let plan = Arc::clone(&graph.lock().await.stages[stage_id as usize].plan);
            Ok(physical_plan_to_bytes(plan)
                .expect("Failed to serialize physical plan")
                .to_vec())
        } else {
            Err(SchedulerError::Error("Graph not found.".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::fs;
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};
    use tokio::time::sleep;

    use crate::parser::ExecutionPlanParser;
    use crate::{
        composable_database::TaskId,
        query_graph::{QueryGraph, StageStatus},
        queue::{QueryKey, Queue},
    };
    use std::{
        cmp::min,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::SystemTime,
    };

    // Test that query keys compare properly.
    #[tokio::test]
    async fn test_query_key_cmp() {
        let then = SystemTime::now();
        let now1 = SystemTime::now().duration_since(then).unwrap();
        sleep(Duration::from_secs(1)).await;
        let now2 = SystemTime::now().duration_since(then).unwrap();

        let key1 = QueryKey {
            ft: now1.clone(),
            qid: 1,
        };
        let key2 = QueryKey { ft: now2, qid: 0 };
        let key3 = QueryKey { ft: now1, qid: 0 };
        // Make sure durations are compared first
        assert!(key1 < key2);
        // Then qids
        assert!(key3 < key1);
    }

    // Deprecated, use test_queue_conc instead
    #[tokio::test]
    async fn test_queue() {
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql/expr.slt");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/");
        let mut queue = Box::new(Queue::new(Arc::new(Notify::new())));
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_scheduler: Testing file {}", test_file);
        if let Ok(physical_plans) = parser.get_execution_plan_from_file(&test_file).await {
            let mut qid = 0;
            // Add a bunch of queries
            for plan in &physical_plans {
                let graph = QueryGraph::new(qid, Arc::clone(plan)).await;
                queue.add_query(qid, Arc::new(Mutex::new(graph))).await;
                qid += 1;
            }
            let mut tasks: Vec<TaskId> = Vec::new();
            for _ in 0..qid {
                tasks.push(queue.next_task().await.expect("No tasks left in queue."));
            }
            for _ in 0..qid {
                queue
                    .remove_task(tasks.pop().unwrap(), StageStatus::Finished(0))
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
        let queue = Arc::new(Mutex::new(Queue::new(Arc::new(Notify::new()))));
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_queue_conc: Testing files in {}", test_sql_dir);

        let mut physical_plans = Vec::new();
        for file in fs::read_dir(test_sql_dir).unwrap() {
            let path_buf = file.unwrap().path();
            let path = path_buf.to_str().unwrap();
            physical_plans.extend(parser.get_execution_plan_from_file(&path).await.unwrap());
        }

        // let physical_plans = parser
        //     .get_execution_plan_from_file(&test_file)
        //     .await
        //     .unwrap();
        println!("Got {} plans.", physical_plans.len());
        let nplans = min(physical_plans.len(), 400);
        let plans = physical_plans[..nplans].to_vec();
        let qid: Arc<Mutex<AtomicU64>> = Arc::new(Mutex::new(AtomicU64::new(0)));

        // Add a bunch of queries
        let mut jobs = Vec::new();
        for plan in plans {
            let queue_clone = Arc::clone(&queue);
            let qid_clone = Arc::clone(&qid);
            // Spawn threads that each enqueue a job
            jobs.push(tokio::spawn(async move {
                let query_id = qid_clone.lock().await.fetch_add(1, Ordering::SeqCst);
                let graph = QueryGraph::new(query_id, Arc::clone(&plan)).await;
                queue_clone
                    .lock()
                    .await
                    .add_query(query_id, Arc::new(Mutex::new(graph)))
                    .await;
            }));
        }

        // spawn as many tasks as there are queries.
        // WARNING: may need more as pipelines are added.
        for _ in 0..nplans {
            let queue_clone = Arc::clone(&queue);
            jobs.push(tokio::spawn(async move {
                // Get a plan, looping until one is available
                loop {
                    let task_opt = queue_clone.lock().await.next_task().await;
                    if let Some(task) = task_opt {
                        queue_clone
                            .lock()
                            .await
                            .remove_task(task, StageStatus::Finished(0))
                            .await;
                    }
                    let time = rand::thread_rng().gen_range(200..4000);
                    tokio::time::sleep(tokio::time::Duration::from_millis(time)).await;
                    // Return if no more queries left.
                    if queue_clone.lock().await.queue.len() == 0 {
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
        assert!(Arc::clone(&queue).lock().await.next_task().await.is_none());
        assert!(queue.lock().await.size() == 0);
        println!("Finished {:?} tasks.", nplans);
    }
}
