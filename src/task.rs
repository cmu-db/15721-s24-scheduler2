use crate::server::composable_database::TaskId;
use std::time::SystemTime;

// TODO: some of these don't do anything since
// the task is only created when it is ready
#[derive(Debug, Clone)]
pub enum TaskStatus {
    Ready,
    Running(SystemTime), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub(crate) task_id: TaskId,
    pub(crate) status: TaskStatus, // TODO: unused?
}

const HANDSHAKE_QUERY_ID: u64 = u64::MAX;
const HANDSHAKE_TASK_ID: u64 = u64::MAX;
const HANDSHAKE_STAGE_ID: u64 = u64::MAX;
const HANDSHAKE_TASK: TaskId = TaskId {
    query_id: HANDSHAKE_QUERY_ID,
    task_id: HANDSHAKE_TASK_ID,
    stage_id: HANDSHAKE_STAGE_ID,
};
