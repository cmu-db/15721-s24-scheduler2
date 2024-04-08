use std::process::Command;

fn main() -> Result<(), String> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/common.proto"], &["proto"])
        .map_err(|e| format!("Failed to compile protos {:?}", e))?;

    println!("cargo:rerun-if-changed=build.rs");

    let tpch_data_downloader = "./download_tpch_parquet_data.py";

    let output = Command::new("python")
        .arg(tpch_data_downloader)
        .output()
        .expect("Failed to execute Python script");

    if !output.status.success() {
        panic!(
            "Python script failed with output: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}
