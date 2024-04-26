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

const HANDSHAKE_TASK_ID: TaskId = TaskId {
    query_id: u64::MAX,
    task_id: u64::MAX,
    stage_id: u64::MAX,
};

impl TaskId {
    pub fn is_handshake(&self) -> bool {
        return self.eq(&HANDSHAKE_TASK_ID);
    }
}
