use crate::query_graph::{QueryGraph, StageStatus};
use crate::server::composable_database::TaskId;
use crate::task::{
    Task,
    TaskStatus::{self, *},
};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

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

// TODO: only use ft for comparisons
#[derive(Debug, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct QueryKey {
    // pub running: u64,
    pub ft: Duration,
    pub qid: u64,
}

#[derive(Debug)]
pub struct Queue {
    // The queue used to order queries by executor usage.
    queue: BTreeMap<QueryKey, Arc<Mutex<QueryGraph>>>,
    // The startup time of the queue, used to calculate new global passes.
    start_ts: SystemTime,
    // Structure that maps query IDs to query keys.
    query_map: HashMap<u64, (Arc<QueryKey>, Arc<Mutex<QueryGraph>>)>,
    // List of currently running tasks.
    running_task_map: HashMap<TaskId, Task>,
}

impl Queue {
    pub fn new() -> Self {
        Self {
            queue: BTreeMap::new(),
            start_ts: SystemTime::now(),
            query_map: HashMap::new(),
            running_task_map: HashMap::new(),
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
        let graph = self.query_map.get(&key.qid).unwrap();
        // Temporarily remove query from queue, if present, and get its graph
        // TODO: only do this if the query key was changed?
        let _ = self.queue.remove(&key);

        match graph
            .lock()
            .await
            .update_stage_status(finished_stage_id, finished_stage_status)
            .unwrap()
        {
            // If query still has available tasks, re-insert with updated priority
            Available => self.queue.insert(&key, Arc::clone(&graph)),
            // If query is unavailable, do not re-insert
            Waiting => {}
            // If query is done, do not re-insert and remove from query map
            Done => self.query_map.remove(&key.qid).expect("Query not found."),
        }
    }

    // Mark this task as running.
    async fn add_running_task(&mut self, task: Task, key: Arc<QueryKey>) {
        // Change the task's status to running.
        task.status = Running(SystemTime::now());
        // Add the task to the list of running tasks.
        self.running_task_map.insert(&task.task_id, key);
        // Send the update to the query graph and reorder queue.
        // WARNING: stage_status may not be 'running' if tasks and stages are not 1:1
        self.update_status(key, task.task_id.stage_id, StageStatus::Running(0))
            .await;
    }

    /* Get the minimum element of the queue, or None if the queue is empty */
    fn min(&mut self) -> Option<(QueryKey, Arc<Mutex<QueryGraph>>)> {
        self.queue.pop_first()
    }

    #[cfg(test)]
    pub async fn size(&self) -> usize {
        self.queue.len()
    }

    // TODO: take querygraph instead of qid
    pub async fn new_query(&mut self, qid: u64, graph: Arc<Mutex<QueryGraph>>) {
        let key = QueryKey {
            // running: 0,
            ft: SystemTime::now().duration_since(self.start_ts).unwrap(),
            qid,
        };
        self.query_map
            .insert(qid, (Arc::new(key), Arc::clone(&graph)));
        self.queue.insert(key, Arc::clone(&graph));
    }

    /*
    Remove this task from the list of running tasks and mark it as done.
    This function forwards task info to the task's query graph,
    updating it if necessary.
    */
    pub async fn remove_task(
        &mut self,
        task: Task,
        finished_stage_id: u64,
        finished_stage_status: StageStatus,
    ) {
        // Remove the task from the running map.
        let _ = self.running_task_map.remove(&task.task_id).unwrap();
        // Get the query ID.
        let query = task.task_id.query_id;
        // Get the key corresponding to the task's query.
        let key = self.query_map.get(&query).unwrap();
        // Ensure the task is running.
        if let Running(start_ts) = task.status {
            // Increment the query's pass using the task's elapsed time.
            key.ft += SystemTime::now().duration_since(start_ts).unwrap();
            self.update_status(*key, finished_stage_id, finished_stage_status)
        } else {
            assert!(false);
        }
    }

    /*
    Return the next task, or None if the queue is empty.
    Blocking is handled in the server.
    */
    pub async fn next_task(&mut self) -> Option<TaskId> {
        // Get the highest priority query.
        match self.min() {
            Some((key, graph)) => {
                // If a query is available, get its next task
                let new_task = graph.lock().await.next_task().await;
                debug_assert!(matches!(new_task.status, TaskStatus::Ready));
                self.add_running_task(new_task, key);
                Some(new_task.task_id)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
  use crate::queue::QueryKey;
  use std::time::SystemTime;

  #[tokio::test]
  async fn test_query_key_cmp() {
    let then = SystemTime::now();
    let key1 = QueryKey {
      ft: SystemTime::now().duration_since(then).unwrap(),
      qid: 0
    };
    let key2 = QueryKey {
      ft: SystemTime::now().duration_since(then).unwrap(),
      qid: 0
    };
    assert!(key1 < key2);
  }
}
