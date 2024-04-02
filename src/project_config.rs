use datafusion::prelude::{CsvReadOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::Arc;
use walkdir::WalkDir;

// Format definitions for the config file
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub(crate) scheduler: Scheduler,
    pub(crate) executors: Executor,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Scheduler {
    pub(crate) id_addr: String,
    pub(crate) port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Executor {
    pub(crate) id: u8,
    ip_addr: String,
    port: u16,
    numa_node: u8,
}

pub fn read_config(config_path: &str) -> Config {
    let config_str = fs::read_to_string(config_path).expect("Failed to read config file");
    toml::from_str(&config_str).expect("Failed to parse config file")
}

pub async fn load_catalog(catalog_path: &str) -> Arc<SessionContext> {
    let ctx = SessionContext::new();
    // Bulk load csv files in the context (simulating API calls to the catalog)
    for entry in WalkDir::new(catalog_path) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() && entry.path().extension().map_or(false, |e| e == "csv") {
            let file_path = entry.path().to_str().unwrap();
            // Extract the table name from the file name without the extension
            let table_name = entry.path().file_stem().unwrap().to_str().unwrap();

            let options = CsvReadOptions::new();
            // Register the CSV file as a table
            let result = ctx.register_csv(table_name, file_path, options).await;
            assert!(
                result.is_ok(),
                "Failed to register CSV file: {:?}",
                file_path
            );
        }
    }

    Arc::new(ctx)
}

#[cfg(test)]
mod tests {

    use crate::project_config::read_config;
    #[test]
    pub fn test_read_config() {
        let config = read_config();
        assert_eq!("127.0.0.1", config.scheduler.id_addr);
    }
}
