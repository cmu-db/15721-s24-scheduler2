#![allow(dead_code)]
use crate::composable_database::{QueryStatus, TaskId};
use crate::task::{Task, TaskStatus};
use crate::task_queue::TaskQueue;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::JoinSide;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
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
    Waiting,
    Runnable,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
}

// Status of a query with respect to the task queue.
#[derive(Clone, Debug, PartialEq)]
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
    task_queue: TaskQueue,  // Ready tasks in this graph
}

impl QueryGraph {
    pub async fn new(query_id: u64, plan: Arc<dyn ExecutionPlan>) -> Self {
        // Build stages.
        // To run this configuration, use 'cargo build --features stages'.
        let mut builder = GraphBuilder::new();
        let stages = builder.build(plan.clone());
        println!("QueryGraph::new: generated {} stages.", stages.len());
        println!("{:#?}", stages);

        let mut query = Self {
            query_id,
            status: QueryStatus::InProgress,
            tid_counter: AtomicU64::new(0),
            stages,
            task_queue: TaskQueue::new(),
        };

        // Build tasks for leaf stages.
        for (i, stage) in query.stages.iter().enumerate() {
            if let StageStatus::Runnable = stage.status {
                let task = Task::new(query_id, i as u64, query.next_task_id());
                query.task_queue.add_tasks(vec![task]);
            }
        }
        query
    }

    #[inline]
    fn next_task_id(&self) -> u64 {
        self.tid_counter.fetch_add(1, Ordering::SeqCst)
    }

    // Returns whether the query is done, waiting for tasks, or has tasks ready
    pub fn get_queue_status(&self) -> QueryQueueStatus {
        // If the query is done
        if self.status == QueryStatus::Done {
            return QueryQueueStatus::Done;
        }
        // If the query's queue has no available tasks
        if self.task_queue.size() == 0 {
            return QueryQueueStatus::Waiting;
        }
        // If the query has available tasks
        return QueryQueueStatus::Available;
    }

    #[inline]
    pub fn next_task(&mut self) -> Task {
        self.task_queue.next_task()
    }

    pub fn abort(&mut self) {
        self.status = QueryStatus::Failed;
    }

    pub fn update_stage_status(
        &mut self,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<(), &'static str> {
        if let Some(stage) = self.stages.get_mut(stage_id as usize) {
            match (&stage.status, &status) {
                (StageStatus::Waiting, StageStatus::Runnable) => {
                    stage.status = status;
                }
                (StageStatus::Runnable, StageStatus::Running(_a)) => {
                    stage.status = status;
                }
                (StageStatus::Running(_a), StageStatus::Finished(_b)) => {
                    stage.status = status;
                    let outputs = stage.outputs.clone();

                    if outputs.is_empty() {
                        self.status = QueryStatus::Done;
                        return Ok(());
                    }

                    // Check to see if new stages are runnable.
                    for output_id in &outputs {
                        if self.stages[*output_id as usize]
                            .inputs
                            .iter()
                            .all(|&input_id| {
                                matches!(
                                    self.stages[input_id as usize].status,
                                    StageStatus::Finished(_)
                                )
                            })
                        {
                            self.update_stage_status(*output_id, StageStatus::Runnable)?;
                            let new_output_task =
                                Task::new(self.query_id, *output_id, self.next_task_id());
                            self.task_queue.add_tasks(vec![new_output_task]);
                        }
                    }
                }
                s => {
                    let msg = format!("Mismatched stage statuses for stage {}. Got: {:?}", stage_id, s.clone()).leak();
                    return Err(msg);
                }
            }
            Ok(())
        } else {
            Err("Task not found.")
        }
    }

    // fn build_tasks(&mut self)
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

    // fn add_output(&mut self, output: u64) {
    //     self.outputs.push(output)
    // }
}

impl Into<QueryStage> for Pipeline {
    fn into(self) -> QueryStage {
        let status = if self.inputs.is_empty() {
            StageStatus::Runnable
        } else {
            StageStatus::Waiting
        };

        QueryStage {
            plan: self.plan.unwrap(),
            status,
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
        // if plan.as_any().is::<HashJoinExec>()
        //     || plan.as_any().is::<SortMergeJoinExec>()
        //     || plan.as_any().is::<NestedLoopJoinExec>()
        //     || plan.as_any().is::<CrossJoinExec>()
        //     || plan.as_any().is::<SymmetricHashJoinExec>()
        if plan.children().len() == 2 {
            let left = plan.children()[0].clone();
            let right = plan.children()[1].clone();

            // Switch left and right for SortMergeJoinExec

            // Left <-> Build Side
            let child_pipeline = self.add_pipeline(Some(pipeline));

            let left_plan = self.parse_plan(left, child_pipeline);
            let left_results = Self::pipeline_breaker(left_plan.schema(), child_pipeline);
            children.push(left_results);

            self.pipelines[child_pipeline].set_plan(left_plan);

            // Right <-> Probe Side
            let right_plan = self.parse_plan(right, pipeline);
            children.push(right_plan);
        // } else if plan.as_any().is::<GlobalLimitExec>()
        //     || plan.as_any().is::<SortExec>()
        //     || plan.as_any().is::<AggregateExec>()
        // {
        //     let child = plan.children()[0].clone();
        //     let child_pipeline = self.add_pipeline(Some(pipeline));
        //
        //     let child_plan = self.parse_plan(child, child_pipeline);
        //     let child_results = Self::pipeline_breaker(plan.schema(), child_pipeline);
        //     children.push(child_results);
        //
        //     self.pipelines[child_pipeline].set_plan(child_plan);
        } else {
            if let [ref child] = plan.children()[..] {
                let child_plan = self.parse_plan(child.clone(), pipeline);
                children.push(child_plan);
            }
        }

        // match plan.children()[..] {
        //     [ref left, ref right] => {
        //         // Left <-> Build Side
        //         let child_pipeline = self.add_pipeline(Some(pipeline));
        //         let child_results = Self::pipeline_breaker(plan.schema(), child_pipeline);
        //         children.push(child_results);
        //
        //         let child_plan = self.parse_plan(left, child_pipeline);
        //         self.pipelines[child_pipeline].set_plan(child_plan);
        //
        //         // Right <-> Probe Side
        //         let main_plan = self.parse_plan(right, pipeline);
        //         children.push(main_plan);
        //     }
        //     [ref child] => {
        //         if true {
        //             let child_pipeline = self.add_pipeline(Some(pipeline));
        //             let child_results = Self::pipeline_breaker(plan.schema(), child_pipeline);
        //             children.push(child_results);
        //
        //             let child_plan = self.parse_plan(child.clone(), child_pipeline);
        //             self.pipelines[child_pipeline].set_plan(child_plan);
        //         } else {
        //             let child_plan = self.parse_plan(child.clone(), pipeline);
        //             children.push(child_plan);
        //         }
        //     }
        //     [] => (),
        //     _ => unreachable!(),
        // }

        with_new_children_if_necessary(plan, children)
            .unwrap()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ExecutionPlanParser;

    fn verify_stages(stages: Vec<QueryStage>) {
        for i in 0..stages.len() {
            let stage_idx = i as u64;
            let stage = &stages[i];
            let outputs = stage.outputs.clone();
            let inputs = stage.inputs.clone();
            // Check outputs
            for output in outputs {
                assert_ne!(stage_idx, output); // cycle
                let output_stage = &stages[output as usize];
                // make sure that inputs/outputs agree
                assert!(output_stage.inputs.contains(&stage_idx));
            }
            // Check inputs
            for input in inputs {
                assert_ne!(stage_idx, input); // cycle
                let input_stage = &stages[input as usize];
                // make sure that inputs/outputs agree
                assert!(input_stage.outputs.contains(&stage_idx));
            }
        }
        println!("Plan parsed into {} stages... ok.", stages.len());
    }

    #[tokio::test]
    async fn test_builder() {
        // Test that inputs/outputs match
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql/7.sql");
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/");
        let parser = ExecutionPlanParser::new(catalog_path).await;
        println!("test_scheduler: Testing file {}", test_file);
        let physical_plans = parser
            .get_execution_plan_from_file(&test_file)
            .await
            .expect("Could not get execution plan from file.");
        // Add a bunch of queries
        println!("Read {} query plans.", physical_plans.len());
        for plan in &physical_plans {
            let mut builder = GraphBuilder::new();
            let stages = builder.build(plan.clone());
            verify_stages(stages);
        }
    }
}
