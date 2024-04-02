use crate::api::composable_database::TaskId;

#[derive(Debug)]
pub enum TaskStatus {
    Waiting,
    Ready,
    Running(u32), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

#[derive(Debug)]
pub struct Task {
    pub(crate) task_id: TaskId,
    pub(crate) status: TaskStatus,
}
