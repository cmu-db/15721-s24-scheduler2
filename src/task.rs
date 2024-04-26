use crate::server::composable_database::TaskId;
use std::time::SystemTime;

// TODO: some of these don't do anything since
// the task is only created when it is ready
#[derive(Debug)]
pub enum TaskStatus {
    Waiting,
    Ready,
    Running(SystemTime), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

#[derive(Debug, Copy)]
pub struct Task {
    pub(crate) task_id: TaskId,
    pub(crate) status: TaskStatus, // TODO: unused?
}
