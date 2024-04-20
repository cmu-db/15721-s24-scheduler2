use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use walkdir::WalkDir;

// Format definitions for the config file
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub(crate) scheduler: SchedulerConfig,
    pub(crate) executors: Vec<ExecutorConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub(crate) id_addr: String,
    pub(crate) port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub(crate) id: i32,
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
    // Bulk load files in the context (simulating API calls to the catalog)
    for entry in WalkDir::new(catalog_path) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            let file_path = entry.path();
            let table_name = file_path.file_stem().unwrap().to_str().unwrap();

            let extension = file_path.extension().and_then(OsStr::to_str);

            match extension {
                Some("csv") => {
                    let options = CsvReadOptions::new();
                    let result = ctx
                        .register_csv(table_name, file_path.to_str().unwrap(), options)
                        .await;
                    assert!(
                        result.is_ok(),
                        "Failed to register CSV file: {:?}",
                        file_path
                    );
                }
                Some("parquet") => {
                    let options = ParquetReadOptions::default();
                    let result = ctx
                        .register_parquet(table_name, file_path.to_str().unwrap(), options)
                        .await;
                    eprintln!(
                        "Registered {}, at path {:?}",
                        table_name,
                        file_path.to_str().as_slice()
                    );
                    assert!(
                        result.is_ok(),
                        "Failed to register Parquet file: {:?}",
                        file_path
                    );
                }
                _ => {}
            }
        }
    }

    Arc::new(ctx)
}

#[cfg(test)]
mod tests {

    use crate::project_config::read_config;
    #[test]
    pub fn test_read_config() {
        let config = read_config("./executors.toml");
        assert_eq!("0.0.0.0", config.scheduler.id_addr);
    }
}
