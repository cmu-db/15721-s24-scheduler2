use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, process};
use walkdir::WalkDir;

fn main() -> Result<(), String> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/common.proto"], &["proto"])
        .map_err(|e| format!("Failed to compile protos {:?}", e))?;

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let test_data_dir = Path::new(&manifest_dir).join("test_data");
    let parquet_files = WalkDir::new(test_data_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .count();

    const NUM_FILE_TPCH: usize = 8;
    if parquet_files < NUM_FILE_TPCH {
        panic!("Build failed, please download data first by running download_tpch_parquet_data.py");
    }

    Ok(())
}
