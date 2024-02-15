#![allow(dead_code)]

use std::collections::{VecDeque, HashMap};

use crate::query_graph::{QueryGraph, StageStatus};

enum TaskStatus {
    Waiting,
    Ready,
    Running(u32), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

pub struct Task {
    id: u64,
    query_id: u64,
    stage_id: usize,
    status: TaskStatus,
}

pub struct Scheduler {
    // Internal state of queries.
    query_table: HashMap<u64, QueryGraph>,

    // Task queue
    task_queue: VecDeque<Task>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            query_table: HashMap::new(),
            task_queue: VecDeque::new(),
        }
    }

    pub fn schedule_plan(&mut self, query_id: u64, plan: RelType) {
        // Build a query graph from the plan.

        let query_id = 1; // Should generate a unique query id.
        let query = QueryGraph::new(query_id, plan);
        self.query_table.insert(query_id, plan);

        // Add the query to the task queue.
    }

    pub fn update_stage_status(&mut self, query_id: u64, stage_id: usize, status: StageStatus) {
        // Update the status of the stage in the query graph.
    }

}
