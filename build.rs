// use std::io::Result;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .out_dir("src")
        .compile_protos(&["proto/common.proto"], &["proto"])?;

    tonic_build::configure()
        .build_server(true) // build server side code only
        .build_client(false)
        .compile(
            &["proto/common.proto"],
            &["proto"], // Specify the directory where .proto files reside
        )?;
    Ok(())
}
