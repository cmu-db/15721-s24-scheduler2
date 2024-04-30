use crate::composable_database::{QueryStatus, TaskId};
use crate::query_graph::{QueryGraph, QueryQueueStatus, StageStatus};
use crate::task::{
    Task,
    TaskStatus::{self, *},
};
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, Notify};

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

// TODO: only use ft for comparisons
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct QueryKey {
    // pub running: u64,
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
    // List of currently running tasks.
    running_task_map: HashMap<TaskId, Task>,
    // Notify primitive that signals when new tasks are ready.
    avail: Arc<Notify>,
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
        key: QueryKey,
        finished_stage_id: u64,
        finished_stage_status: StageStatus,
    ) {
        // Get the graph for this query
        let graph = Arc::clone(&self.query_map.get(&key.qid).unwrap().1);
        // Temporarily remove query from queue, if present, and get its graph
        // TODO: only do this if the query key was changed?
        let _ = self.queue.remove(&key);

        // If graph has more tasks available, re-insert query and notify
        if graph
            .lock()
            .await
            .update_stage_status(finished_stage_id, finished_stage_status)
            .await
            .unwrap()
            == QueryQueueStatus::Available
        {
            self.queue.insert(key);
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
        self.update_status(key, task.task_id.stage_id, StageStatus::Running(0))
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
            // Increment the query's pass using the task's elapsed time.
            (*key).ft += SystemTime::now().duration_since(start_ts).unwrap();
            let key_copy = *key;
            drop(key); // to avoid double mutable borrow
            self.update_status(key_copy, task_id.stage_id, finished_stage_status)
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
        // Get the highest priority query.
        match self.min() {
            Some(key) => {
                // If a query is available, get its next task
                let graph = &self.query_map.get(&key.qid).unwrap().1;
                let new_task = graph.lock().await.next_task().await;
                debug_assert!(matches!(new_task.status, TaskStatus::Ready));
                self.add_running_task(new_task.clone(), key).await;
                Some(new_task.task_id)
            }
            None => None,
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
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use tokio::sync::{Mutex, Notify};
    use tokio::time::sleep;
    use std::time::Duration;

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
        let key2 = QueryKey {
            ft: now2,
            qid: 0,
        };
        let key3 = QueryKey {
            ft: now1,
            qid: 0,
        };
        // Make sure durations are compared first
        assert!(key1 < key2);
        // Then qids
        assert!(key3 < key1);
    }

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
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/expr.slt");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/");
        let queue = Arc::new(Mutex::new(Queue::new(Arc::new(Notify::new()))));
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_queue_conc: Testing file {}", test_file);
        if let Ok(physical_plans) = parser.get_execution_plan_from_file(&test_file).await {
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
                            return;
                        }
                        let time = rand::thread_rng().gen_range(200..4000);
                        tokio::time::sleep(tokio::time::Duration::from_millis(time)).await;
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
}
