use tokio::sync::{Mutex, Notify};
use std::collections::VecDeque;
use crate::task::Task;

#[derive(Debug)]
pub struct TaskQueue {
    queue: Mutex<VecDeque<Task>>,
    pub avail: Notify,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            avail: Notify::new(),
        }
    }

    pub async fn size(&self) -> usize {
        self.queue.lock().await.len()
    }

    pub async fn add_tasks(&self, tasks: Vec<Task>) -> bool {
        let task_count = tasks.len();
        if task_count == 0 {
            return false;
        }

        let mut queue = self.queue.lock().await;
        queue.extend(tasks);

        if task_count == 1 {
            self.avail.notify_one();
        } else {
            self.avail.notify_waiters();
        }

        true
    }

    pub async fn next_task(&self) -> Task {
        let mut queue = self.queue.lock().await;
        while queue.is_empty() {
            drop(queue); // Drop the lock before waiting
            self.avail.notified().await;
            queue = self.queue.lock().await; // Re-acquire the lock after being notified
        }
        queue.pop_front().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use std::sync::Arc;
    use crate::server::composable_database::TaskId;
    use crate::task::TaskStatus;

    fn create_task(task_id: u64) -> Task {
        Task {
            task_id: TaskId { query_id: task_id, task_id, stage_id: 0 },
            status: TaskStatus::Ready,
        }
    }

    #[test]
    fn test_new_queue() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let queue = TaskQueue::new();
            assert_eq!(queue.size().await, 0);
        });
    }

    #[test]
    fn test_add_and_size() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let queue = TaskQueue::new();
            assert!(queue.add_tasks(vec![create_task(1)]).await);
            assert_eq!(queue.size().await, 1);
        });
    }

    #[test]
    fn test_next_task() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let queue = Arc::new(TaskQueue::new());
            queue.add_tasks(vec![create_task(1), create_task(2)]).await;

            let queue_clone = queue.clone();
            let handle = tokio::spawn(async move {
                assert_eq!(queue_clone.next_task().await.task_id, TaskId{ query_id: 1, task_id: 1, stage_id: 0 });
                assert_eq!(queue_clone.next_task().await.task_id, TaskId{ query_id: 2, task_id: 2, stage_id: 0 });
            });

            handle.await.unwrap();
        });
    }
}

