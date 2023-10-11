mod registry;
mod schema;
mod server;

use std::env;

use tracing::info;
use tracing_subscriber;

use crate::registry::Registry;
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }

    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9999".parse().unwrap();
    let seeds = "debian-gnu-linux:19092";
    let rpfs = server::RedpandaFlightService::new(seeds, "_schema");

    let svc = FlightServiceServer::new(rpfs);

    info!("starting with address {}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    info!("bye!");
    Ok(())
}
