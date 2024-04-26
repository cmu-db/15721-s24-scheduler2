use crate::server::composable_database::{QueryStatus, TaskId};
use crate::task::{Task, TaskStatus};
use crate::task_queue::TaskQueue;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// TODO Change to Waiting, Ready, Running(vec[taskid]), Finished(vec[locations?])
#[derive(Clone, Debug, Default)]
pub enum StageStatus {
    #[default]
    NotStarted,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
}

// Status of a query with respect to the task queue.
pub enum QueryQueueStatus {
    Available, // Query has available tasks.
    Waiting,   // Query has no available tasks.
    Done,      // Query has finished all tasks.
}

#[derive(Clone, Debug)]
pub struct QueryStage {
    pub plan: Arc<dyn ExecutionPlan>,
    status: StageStatus,
    outputs: Vec<u64>,
    inputs: Vec<u64>,
}
#[derive(Debug)]
pub struct QueryGraph {
    pub query_id: u64,
    pub status: QueryStatus,
    tid_counter: AtomicU64, // TODO: add mutex to stages and make elements pointers to avoid copying
    pub stages: Vec<QueryStage>, // Can be a vec since all stages in a query are enumerated from 0.
    plan: Arc<dyn ExecutionPlan>, // Potentially can be thrown away at this point.
    task_queue: Arc<TaskQueue>, // Ready tasks in this graph
}

impl QueryGraph {
    pub fn new(query_id: u64, plan: Arc<dyn ExecutionPlan>) -> Self {
        let mut builder = GraphBuilder::new();
        let stages = builder.build(plan.clone());

        let tid_counter = AtomicU64::new(0);
        let id = tid_counter.fetch_add(1, Ordering::SeqCst);
        // for now, create single task
        let task = Task {
            task_id: TaskId {
                task_id: id,
                query_id,
                stage_id: id,
            },
            status: TaskStatus::Ready,
        };
        let mut queue = TaskQueue::new();
        queue.add_tasks(vec![task]);
        Self {
            query_id,
            status: QueryStatus::InProgress,
            plan: plan.clone(),
            tid_counter,
            stages: vec![QueryStage {
                plan: plan.clone(),
                status: StageStatus::NotStarted,
                outputs: Vec::new(),
                inputs: Vec::new(),
            }],
            task_queue: Arc::new(queue),
        }
    }

    fn next_task_id(&mut self) -> u64 {
        self.tid_counter.fetch_add(1, Ordering::SeqCst)
    }

    // Returns whether the query is done, waiting for tasks, or has tasks ready
    async fn get_queue_status(&self) -> QueryQueueStatus {
        // If the query is done
        if self.status == QueryStatus::Done {
            return QueryQueueStatus::Done;
        }
        // If the query's queue has no available tasks
        if self.task_queue.size().await == 0 {
            return QueryQueueStatus::Waiting;
        }
        // If the query has available tasks
        return QueryQueueStatus::Available;
    }

    pub async fn next_task(&mut self) -> Task {
        self.task_queue.next_task().await;
    }

    pub async fn update_stage_status(
        &mut self,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<QueryQueueStatus, &'static str> {
        if self.stages.get(stage_id as usize).is_some() {
            // Compare current status against new status
            match (&self.stages.get(stage_id as usize).unwrap().status, &status) {
                // If transition NotStarted -> Running, simply update status
                (StageStatus::NotStarted, StageStatus::Running(_)) => {
                    self.stages.get_mut(stage_id as usize).unwrap().status = status;
                    return Ok(self.get_queue_status().await);
                }
                // If transition Running -> Finished, remove this stage as an input from all of its outputs
                (StageStatus::Running(_a), StageStatus::Finished(_b)) => {
                    // Get list of outputs
                    let outputs = {
                        let stage = self.stages.get_mut(stage_id as usize).unwrap();
                        stage.status = status;
                        stage.outputs.clone()
                    };

                    // If this stage has no outputs, this query is done
                    if outputs.is_empty() {
                        self.status = QueryStatus::Done;
                        // No more to do
                        return Ok(self.get_queue_status().await);
                    }

                    // Remove this stage from each output stage's input stage
                    for output_stage_id in &outputs {
                        let output_stage = self.stages.get_mut(*output_stage_id as usize).unwrap();

                        // Add output stage to queue if it has no inputs
                        if output_stage.inputs.len() == 0 {
                            // Set output stage's status to "running"
                            output_stage.status = StageStatus::Running(0); // TODO: "ready stage status?"
                            let new_output_task = Task {
                                task_id: TaskId {
                                    query_id: self.query_id,
                                    task_id: *output_stage_id,
                                    stage_id: *output_stage_id,
                                },
                                status: TaskStatus::Ready,
                            };
                            self.task_queue.add_tasks(vec![new_output_task]);
                        }
                    }
                    return Ok(self.get_queue_status().await);
                }
                _ => Err("Mismatched stage statuses."),
            }
        } else {
            Err("Task not found.")
        }
    }
}

#[derive(Clone, Debug)]
struct Pipeline {
    plan: Option<Arc<dyn ExecutionPlan>>,
    outputs: Vec<u64>,
    inputs: Vec<u64>,
}

impl Pipeline {
    fn new() -> Self {
        Self {
            plan: None,
            outputs: Vec::new(),
            inputs: Vec::new(),
        }
    }

    fn set_plan(&mut self, plan: Arc<dyn ExecutionPlan>) {
        self.plan = Some(plan);
    }
}

impl Into<QueryStage> for Pipeline {
    fn into(self) -> QueryStage {
        QueryStage {
            plan: self.plan.unwrap(),
            status: StageStatus::NotStarted,
            outputs: self.outputs,
            inputs: self.inputs,
        }
    }
}

struct GraphBuilder {
    pipelines: Vec<Pipeline>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
        }
    }

    fn add_pipeline(&mut self, parent: Option<usize>) -> usize {
        let id = self.pipelines.len();
        let mut pipeline = Pipeline::new();
        if let Some(parent_id) = parent {
            pipeline.outputs.push(parent_id as u64);
            self.pipelines[parent_id].inputs.push(id as u64);
        }
        self.pipelines.push(pipeline);
        id
    }

    fn pipeline_breaker(schema: Arc<Schema>, dep: usize) -> Arc<dyn ExecutionPlan> {
        let metadata = HashMap::from([("IntermediateResult".to_string(), dep.to_string())]);
        let schema = (*schema).clone().with_metadata(metadata);
        Arc::new(PlaceholderRowExec::new(Arc::new(schema)))
    }

    pub fn build(&mut self, plan: Arc<dyn ExecutionPlan>) -> Vec<QueryStage> {
        assert_eq!(self.pipelines.len(), 0);

        let root = self.add_pipeline(None);
        let root_plan = self.parse_plan(plan, root);
        self.pipelines[root].set_plan(root_plan);

        self.pipelines
            .iter()
            .map(|stage| stage.clone().into())
            .collect()
    }

    fn parse_plan(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        pipeline: usize,
    ) -> Arc<dyn ExecutionPlan> {
        if plan.children().is_empty() {
            return plan;
        }

        let mut children = vec![];
        if plan.children().len() > 1
            || plan.as_any().is::<GlobalLimitExec>()
            || plan.as_any().is::<SortExec>()
            || plan.as_any().is::<AggregateExec>()
        {
            for child in plan.children() {
                let child_pipeline = self.add_pipeline(Some(pipeline));
                let child_results = Self::pipeline_breaker(plan.schema(), child_pipeline);
                children.push(child_results);

                let child_plan = self.parse_plan(child, child_pipeline);
                self.pipelines[child_pipeline].set_plan(child_plan);
            }
        } else {
            for child in plan.children() {
                let child_plan = self.parse_plan(child, pipeline);
                children.push(child_plan);
            }
        }
        with_new_children_if_necessary(plan, children)
            .unwrap()
            .into()
    }
}
