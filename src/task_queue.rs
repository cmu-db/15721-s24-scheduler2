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

    pub fn size(&self) -> usize {
        self.queue.len()
    }

    // Add tasks to the queue.
    pub fn add_tasks(&mut self, tasks: Vec<Task>) {
        self.queue.extend(tasks);
    }

    /*
     Get the next task from the queue.
     Due to the structure of the outer queue,
     there queue should always be non-empty when called.
    */
    pub fn next_task(&mut self) -> Task {
        self.queue.pop_front().expect("Queue has no tasks.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composable_database::TaskId;
    use crate::task::TaskStatus;
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use tokio::sync::Mutex;

    fn create_task(task_id: u64) -> Task {
        Task {
            task_id: TaskId {
                query_id: task_id,
                task_id,
                stage_id: 0,
            },
            status: TaskStatus::Ready,
        }
    }

    #[test]
    fn test_new_queue() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let queue = TaskQueue::new();
            assert_eq!(queue.size(), 0);
        });
    }

    #[test]
    fn test_add_and_size() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let mut queue = TaskQueue::new();
            queue.add_tasks(vec![create_task(1)]);
            assert_eq!(queue.size(), 1);
        });
    }

    #[test]
    fn test_next_task() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let queue = Arc::new(Mutex::new(TaskQueue::new()));
            {
                let mut queue_lock = queue.lock().await;
                queue_lock.add_tasks(vec![create_task(1), create_task(2)]);
            }

            let queue_clone = queue.clone();
            let handle = tokio::spawn(async move {
                let mut queue_lock = queue_clone.lock().await;
                assert_eq!(
                    queue_lock.next_task().task_id,
                    TaskId {
                        query_id: 1,
                        task_id: 1,
                        stage_id: 0
                    }
                );
                assert_eq!(
                    queue_lock.next_task().task_id,
                    TaskId {
                        query_id: 2,
                        task_id: 2,
                        stage_id: 0
                    }
                );
            });

            handle.await.unwrap();
        });
    }
}
