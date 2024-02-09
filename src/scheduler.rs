#![allow(dead_code)]

use std::collections::{VecDeque, HashMap};

use crate::query_graph::QueryGraph;

enum TaskStatus {
    Waiting,
    Ready,
    Running(i32), // ID of executor running this task
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

    // Executor state
    workers: Vec<i32>,
}

pub struct Dispatcher {
    // Internal worker state.
    workers: Vec<i32>,
}
