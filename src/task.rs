use crate::server::composable_database::TaskId;

#[derive(Clone, Debug)]
pub enum TaskStatus {
    Waiting,
    Ready,
    Running(u32), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

#[derive(Clone, Debug)]
pub struct Task {
    pub(crate) task_id: TaskId,
    pub(crate) status: TaskStatus,
}
