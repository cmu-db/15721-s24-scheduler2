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

    // Compile Substrait proto files
    config.compile_protos(
        &[
            "substrait/proto/substrait/algebra.proto",
            "substrait/proto/substrait/capabilities.proto",
            "substrait/proto/substrait/extended_expression.proto",
            "substrait/proto/substrait/function.proto",
            "substrait/proto/substrait/parameterized_types.proto",
            "substrait/proto/substrait/plan.proto",
            "substrait/proto/substrait/type.proto",
            "substrait/proto/substrait/type_expressions.proto",
            "substrait/proto/substrait/extensions/extensions.proto",
        ],
        &["substrait/proto"], // Include directory for Substrait proto files
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
