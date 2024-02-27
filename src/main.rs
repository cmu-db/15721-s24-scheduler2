pub mod api;
mod composable_database;
mod dispatcher;
pub mod parser;
mod query_graph;
mod query_table;
mod scheduler;
mod task_queue;

use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;
use tonic::transport::Server;

use crate::api::{composable_database::scheduler_api_server::SchedulerApiServer, SchedulerService};

#[derive(Debug, Deserialize)]
struct Executor {
    #[serde(default)]
    id: u64,
    numa_node: u16,
    ip_addr: String,
    port: u16,
}

#[derive(Debug, Deserialize)]
struct Executors {
    executors: Vec<Executor>,
}

impl Executors {
    fn new() -> Self {
        Executors {
            executors: Vec::new(),
        }
    }

    fn from_file() -> Result<Self, ConfigError> {
        let executors: Result<Executors, _> = Config::builder()
            .add_source(File::new(EXECUTOR_CONFIG, FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize();
        executors
    }
}

const EXECUTOR_CONFIG: &str = "executors.toml";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = env!("CARGO_MANIFEST_DIR");
    println!("Path to crate's root: {}", crate_root);
    let addr = "[::1]:50051".parse()?;
    let scheduler_service = SchedulerService::default();
    Server::builder()
        .add_service(SchedulerApiServer::new(scheduler_service))
        .serve(addr)
        .await?;
    Ok(())
}
