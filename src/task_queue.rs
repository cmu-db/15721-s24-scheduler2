use crate::task::Task;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct TaskQueue {
    queue: VecDeque<Task>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub async fn size(&self) -> usize {
        self.queue.len()
    }

    // Add tasks to the queue.
    pub async fn add_tasks(&self, tasks: Vec<Task>) {
        self.queue.extend(tasks);
    }

    /* 
     Get the next task from the queue.
     Due to the structure of the outer queue, 
     there queue should always be non-empty when called.
    */
    pub async fn next_task(&self) -> Task {
        self.queue.pop_front().expect("Queue has no tasks.")
    }
}
