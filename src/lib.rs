mod executor_client;
pub mod frontend;
pub mod integration_test;
pub mod intermediate_results;
pub mod mock_catalog;
pub mod mock_executor;
mod mock_optimizer;
pub mod parser;
pub mod profiling;
mod query_graph;
mod query_table;
mod queue;
pub mod server;
mod task;
mod task_queue;

pub mod composable_database {
    tonic::include_proto!("composable_database");
}

use datafusion::error::DataFusionError;

pub enum SchedulerError {
    Error(String),
    DfError(DataFusionError),
}
