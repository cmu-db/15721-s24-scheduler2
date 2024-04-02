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
    pub(crate) id: u64,
    pub(crate) query_id: u64,
    pub(crate) stage_id: u64,
    pub(crate) status: TaskStatus,
}
