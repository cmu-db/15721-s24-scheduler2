mod composable_database;
mod dispatcher;
mod parser;
mod query_graph;
mod query_table;
mod scheduler;
mod task_queue;
mod api;

use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

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

fn main() {
    println!("Hello, world!");

    let executors = Executors::from_file().unwrap();
    println!("A config: {:#?}", executors);
}
