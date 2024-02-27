use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_proto::bytes::{physical_plan_from_bytes};
use std::sync::Arc;
use sqllogictest::{Record};
use sqllogictest::{ColumnType};

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

pub async fn deserialize_physical_plan(bytes: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    physical_plan_from_bytes(bytes, &ctx)
}

async fn get_execution_plan_from_file(file_path: &str) -> std::result::Result<Vec<Arc<dyn ExecutionPlan>>, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let sql_statements: Vec<Record<DFColumnType>> = sqllogictest::parse_file(file_path)
        .expect("failed to parse file");

    let mut plans = Vec::new();
    for sql_record in sql_statements {

        let sql_opt = match sql_record {
            Record::Statement { ref sql, .. } | Record::Query { ref sql, .. } => Some(sql),
            _ => None,
        };

        if let Some(sql) = sql_opt {
            let plan_result = ctx.sql(&sql).await;
            let plan = match plan_result {
                Ok(plan) => {
                    plan
                },
                Err(_) => {
                    continue;
                }
            };

            let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
                Ok(plan) => {
                    plan
                },
                Err(_) => {
                    continue;
                }
            };
            plans.push(plan);
        }
    }
    Ok(plans)
}

#[tokio::test]
async fn test_get_execution_plans_from_files() {
    use std::fs;

    // Define the directory that contains the .slt files.
    let dir_path = "./test_files";
    eprintln!("Parsing test files in directory: {}", dir_path);

    // Read the directory contents.
    let entries = fs::read_dir(dir_path)
        .expect("Failed to read directory")
        .filter_map(|entry| entry.ok()) // Filter out Err results and unwrap Ok values.
        .map(|entry| entry.path()) // Convert DirEntry to PathBuf.
        .filter(|path| path.extension().and_then(std::ffi::OsStr::to_str) == Some("slt")) // Keep only .slt files.
        .collect::<Vec<_>>();

    // Check if there are any .slt files to process.
    if entries.is_empty() {
        eprintln!("No .slt files found in directory: {}", dir_path);
        return;
    }

    for file_path in entries {
        let file_path_str = file_path.to_str().expect("Failed to convert path to string");
        eprintln!("Processing test file: {}", file_path_str);

        match get_execution_plan_from_file(file_path_str).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("Failed to get execution plans from file {}: {}", file_path_str, e);
                panic!("Test failed due to error with file: {}", file_path_str);
            }
        }
    }
}