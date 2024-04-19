use tonic::transport::Server;

use scheduler2::composable_database::scheduler_api_server::SchedulerApiServer;
use scheduler2::server::SchedulerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:15721".parse().unwrap();

    let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/");
    let scheduler_service = SchedulerService::new(catalog_path).await;

    let server = SchedulerApiServer::new(scheduler_service);
    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
