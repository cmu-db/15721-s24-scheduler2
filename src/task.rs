use crate::server::composable_database::TaskId;
use std::time::SystemTime;

#[derive(Debug)]
pub enum TaskStatus {
    Waiting,
    Ready,
    Running(SystemTime), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

#[derive(Debug)]
pub struct Task {
    pub(crate) task_id: TaskId,
    pub(crate) status: TaskStatus, // TODO: unused?
}
