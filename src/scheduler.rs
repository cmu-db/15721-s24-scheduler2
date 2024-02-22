#![allow(dead_code)]

use std::sync::Arc;

use crate::query_graph::{QueryGraph, StageStatus};
use crate::query_table::QueryTable;
use crate::task_queue::TaskQueue;
use datafusion::physical_plan::ExecutionPlan;

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
    stage_id: u64,
    status: TaskStatus,
}

pub struct Scheduler {
    // Internal state of queries.
    query_table: QueryTable,

    // Task queue
    task_queue: TaskQueue,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            query_table: QueryTable::new(),
            task_queue: TaskQueue::new(),
        }
    }

    pub fn schedule_plan(&mut self, query_id: u64, plan: Arc<dyn ExecutionPlan>) {

        // Build a query graph from the plan.
        let query = QueryGraph::new(query_id, plan);
        let frontier = self.query_table.add_query(query);

        // Add the query to the task queue.
        self.task_queue.add_tasks(frontier);
    }

    pub fn update_stage_status(&mut self, query_id: u64, stage_id: u64, status: StageStatus) {
        // Update the status of the stage in the query graph.
        self.query_table.update_stage_status(query_id, stage_id, status);
    }

}
