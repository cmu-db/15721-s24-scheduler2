use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
use sqllogictest::ColumnType;
use sqllogictest::Record;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DFColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for DFColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'D' => Some(Self::DateTime),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::DateTime => 'D',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

pub async fn deserialize_physical_plan(bytes: Vec<u8>) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    physical_plan_from_bytes(&*bytes, &ctx)
}

pub async fn serialize_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
    match physical_plan_to_bytes(plan) {
        Ok(plan_bytes) => Ok(Vec::from(plan_bytes)),
        Err(e) => Err(e),
    }
}

pub async fn get_execution_plan_from_file(
    file_path: &str,
) -> std::result::Result<Vec<Arc<dyn ExecutionPlan>>, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let sql_statements: Vec<Record<DFColumnType>> =
        sqllogictest::parse_file(file_path).expect("failed to parse file");

    let mut plans = Vec::new();
    for sql_record in sql_statements {
        let sql_opt = match sql_record {
            Record::Statement { ref sql, .. } | Record::Query { ref sql, .. } => Some(sql),
            _ => None,
        };

        if let Some(sql) = sql_opt {
            let plan_result = ctx.sql(&sql).await;
            let plan = match plan_result {
                Ok(plan) => plan,
                Err(_) => {
                    continue;
                }
            };

            let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
                Ok(plan) => plan,
                Err(_) => {
                    continue;
                }
            };
            plans.push(plan);
        }
    }
    Ok(plans)
}

// list all the .slt files under a directory
pub fn list_all_slt_files(dir_path: &str) -> Vec<PathBuf> {
    let entries = fs::read_dir(dir_path)
        .unwrap_or_else(|_| panic!("Failed to read directory: {}", dir_path))
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(std::ffi::OsStr::to_str) == Some("slt"))
        .collect::<Vec<_>>();

    if entries.is_empty() {
        eprintln!("No .slt files found in directory: {}", dir_path);
    }

    entries
}


#[cfg(test)]
mod tests {
    use tonic::Request;
    use crate::api::composable_database::{QueryInfo, ScheduleQueryArgs};
    use crate::parser::{get_execution_plan_from_file, list_all_slt_files, serialize_physical_plan};

    #[tokio::test]
    async fn test_get_execution_plans_from_files() {
        // Define the directory that contains the .slt files.
        let dir_path = "./test_files";
        eprintln!("Parsing test files in directory: {}", dir_path);

        let entries = list_all_slt_files(dir_path);

        // Check if there are any .slt files to process.
        if entries.is_empty() {
            eprintln!("No .slt files found in directory: {}", dir_path);
            return;
        }

        let mut total_execution_plans = 0; // Counter for the total number of execution plans.
        let mut files_scanned = 0; // Counter for the number of files scanned.

        for file_path in entries {
            let file_path_str = file_path
                .to_str()
                .expect("Failed to convert path to string");
            eprintln!("Processing test file: {}", file_path_str);

            match get_execution_plan_from_file(file_path_str).await {
                Ok(plans) => {
                    total_execution_plans += plans.len();
                }
                Err(e) => {
                    eprintln!(
                        "Failed to get execution plans from file {}: {}",
                        file_path_str, e
                    );
                    panic!("Test failed due to error with file: {}", file_path_str);
                }
            }

            files_scanned += 1;
        }

        // Print out the total counts.
        eprintln!(
            "Total number of execution plans generated: {}",
            total_execution_plans
        );
        eprintln!("Total number of files scanned: {}", files_scanned);
    }

    #[tokio::test]
    async fn test_serialize_deserialize_physical_plan() {
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "//select.slt");
        let res = get_execution_plan_from_file(&test_file).await;
        assert!(res.is_ok());
        let plans = res.unwrap();
        for plan in &plans {
            eprintln!("Trying to serialize plan {:?}", plan.clone());
            let serialization_result = serialize_physical_plan(plan.clone()).await;
            assert!(serialization_result.is_ok());
        }
    }
}
