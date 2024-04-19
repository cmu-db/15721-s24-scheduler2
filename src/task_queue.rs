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

    pub async fn next_task(&self) -> Option<Task> {
        let mut queue = self.queue.lock().await;
        while queue.is_empty() {
            drop(queue); // Drop the lock before waiting
            self.avail.notified().await;
            queue = self.queue.lock().await; // Re-acquire the lock after being notified
        }
        queue.pop_front()
    }
}
