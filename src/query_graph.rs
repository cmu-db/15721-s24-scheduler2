use std::collections::HashMap;

use substrait::proto::rel::RelType;

pub enum StageStatus {
    NotStarted,
    Running(u64),
    Finished(u64),
}

pub struct QueryStage {
    stage: RelType,
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

enum TaskStatus {
    Waiting,
    Ready,
    Running(i32), // ID of executor running this task
    Finished,
    Failed,
}


