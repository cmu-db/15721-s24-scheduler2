// use std::io::Result;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.out_dir("src");
    config.compile_protos(
        &[
            "proto/common.proto",
            "proto/optimizer_scheduler.proto",
            "proto/scheduler_executor.proto",
        ],
        &["proto"],
    )?;

    tonic_build::configure()
        .build_server(true) // build server side code only
        .build_client(false)
        .compile(
            &[
                "proto/common.proto",
                "proto/optimizer_scheduler.proto",
                "proto/scheduler_executor.proto",
            ],
            &["proto"], // Specify the directory where .proto files reside
        )?;
    Ok(())
}
