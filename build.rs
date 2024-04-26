fn main() -> Result<(), String> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/common.proto"], &["proto"])
        .map_err(|e| format!("Failed to compile protos {:?}", e))?;
    Ok(())
}
