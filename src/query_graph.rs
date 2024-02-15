#![allow(dead_code)]

// use std::collections::HashMap;

use substrait::proto::rel::RelType;



pub enum StageStatus {
    NotStarted,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
}

pub struct QueryStage {
    plan: RelType,
    status: StageStatus,

    outputs: Vec<usize>,
    inputs: Vec<usize>,
}

pub struct QueryGraph {
    query_id: u64,
    plan: RelType, // Potentially can be thrown away at this point.

    stages: Vec<QueryStage>,
    frontier: Vec<usize>,
}

impl QueryGraph {
    pub fn new(query_id: u64, plan: RelType) -> Self {
        Self {
            query_id,
            plan,
            stages: Vec::new(),
            frontier: Vec::new(),
        }
    }
}

enum TaskStatus {
    Waiting,
    Ready,
    Running(i32), // ID of executor running this task
    Finished,
    Failed,
}
