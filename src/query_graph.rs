#![allow(dead_code)]

use crate::task::{Task, TaskStatus};
use std::collections::HashSet;
use std::ops::DerefMut;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{mem, sync::Arc};
use tokio::sync::RwLock;
use crate::server::composable_database::TaskId;
use crate::query_graph::StageStatus::NotStarted;
use crate::task::TaskStatus::Ready;

// TODO Change to Waiting, Ready, Running(vec[taskid]), Finished(vec[locations?])
#[derive(Clone, Debug, Default)]
pub enum StageStatus {
    #[default]
    NotStarted,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
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
    pub done: bool,
    tid_counter: AtomicU64, // TODO: add mutex to stages and make elements pointers to avoid copying
    pub stages: Vec<QueryStage>, // Can be a vec since all stages in a query are enumerated from 0.
    plan: Arc<dyn ExecutionPlan>, // Potentially can be thrown away at this point.
    frontier: Vec<Task>,
}

impl QueryGraph {
    pub fn new(query_id: u64, plan: Arc<dyn ExecutionPlan>) -> Self {
        let mut builder = GraphBuilder::new();
        let stages = builder.build(plan.clone());

        let tid_counter = AtomicU64::new(0);
        let id = tid_counter.fetch_add(1, Ordering::SeqCst);
        let task = Task {
            task_id: TaskId {
                task_id: id,
                query_id,
                stage_id: id,
            },
            status: Ready,
        };
        Self {
            query_id,
            done: false,
            plan: plan.clone(),
            tid_counter,
            stages: vec![QueryStage{plan: plan.clone(), status: NotStarted, outputs: Vec::new(), inputs: Vec::new()}],
            // stages,
            frontier: vec![task],
        }
    }

    pub fn num_stages(&self) -> u64 {
        self.stages.len() as u64
    }

    fn next_task_id(&mut self) -> u64 {
        self.tid_counter.fetch_add(1, Ordering::SeqCst)
    }

    // Atomically clear frontier vector and return old frontier.
    pub fn get_frontier(&self) -> Vec<Task> {
        self.frontier.clone()
    }

    pub fn update_stage(
        &mut self,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<Vec<Task>, &'static str> {
        Ok(vec![])
    }

    pub fn update_stage_status(
        &mut self,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<(), &'static str> {
        if let Some(stage) = self.stages.get_mut(stage_id as usize) {
            match (&stage.status, &status) {
                // TODO: handle input/output stuff
                (StageStatus::NotStarted, StageStatus::Running(_)) => {
                    stage.status = status;
                    Ok(())
                }
                (StageStatus::Running(_a), StageStatus::Finished(_b)) => {
                    stage.status = status;
                    let outputs = stage.outputs.clone();

                    if outputs.is_empty() {
                        self.done = true;
                    }
                    // stage.status = status;
                    // Remove this stage from each output stage's input stage
                    for output_stage_id in &outputs {
                        if let Some(output_stage) = self.stages.get_mut(*output_stage_id as usize) {
                            // output_stage.inputs.remove(&stage_id);

                            // Add output stage to frontier if its input size is zero
                            if output_stage.inputs.len() == 0 {
                                output_stage.status = StageStatus::Running(0); // TODO: "ready stage status?"
                                let new_output_task = Task {
                                    task_id: TaskId{
                                        query_id: self.query_id,
                                        task_id: *output_stage_id,
                                        stage_id: *output_stage_id
                                    },
                                    status: TaskStatus::Ready,
                                };
                                self.frontier.push(new_output_task);
                            }
                        } else {
                            return Err("Output stage not found.");
                        }
                    }
                    Ok(())
                }

                s => {
                    let msg = format!("Mismatched stage statuses. Got: {:?}", s.clone()).leak();
                    Err(msg)
                }
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

        self.pipelines.iter().map(|stage| stage.clone().into()).collect()
    }

    fn parse_plan(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        pipeline: usize,
    ) -> Arc<dyn ExecutionPlan> {
        if plan.children().is_empty() {
            return plan;
        }

        // TODO: Parse joins into 2 not 3 stages.
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
