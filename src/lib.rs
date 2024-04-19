mod executor;
pub mod integration_test;
pub mod intermediate_results;
mod mock_frontend;
pub mod parser;
pub mod project_config;
mod query_graph;
mod query_table;
mod task;
mod task_queue;
pub mod server;

pub mod composable_database {
    tonic::include_proto!("composable_database");
}

use datafusion::error::DataFusionError;

pub enum SchedulerError {
    Error(String),
    DfError(DataFusionError),
}
