use crate::scheduler::Task;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

#[derive(Debug)]
pub struct TaskQueue {
    queue: Mutex<VecDeque<Task>>,
    pub avail: Condvar,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            avail: Condvar::new(),
        }
    }

    pub fn add_tasks(&self, tasks: Vec<Task>) -> bool {
        let task_count = tasks.len();
        if task_count == 0 {
            return false;
        }
        self.queue.lock().unwrap().extend(tasks);
        if task_count == 1 {
            self.avail.notify_one();
        } else {
            self.avail.notify_all();
        }
        return true;
    }

    pub fn next_task(&self) -> Task {
        let mut queue = self.queue.lock().unwrap();
        // Correct rust syntax?
        while queue.is_empty() {
            queue = self.avail.wait(queue).unwrap();
        }
        queue.pop_front().unwrap()
    }
}
