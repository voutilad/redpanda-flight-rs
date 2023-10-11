use std::env;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber;

mod registry;
mod schema;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    let seeds = env::var("REDPANDA_BROKERS").unwrap_or(String::from("localhost:9092"));
    let topic = env::var("REDPANDA_SCHEMA_TOPIC").unwrap_or(String::from("_schemas"));

    let addr = "127.0.0.1:9999".parse().unwrap();
    info!("starting with address {}", addr);

    let svc = FlightServiceServer::new(server::RedpandaFlightService::new(
        seeds.as_str(),
        topic.as_str(),
    ));
    Server::builder().add_service(svc).serve(addr).await?;

    info!("bye!");
    Ok(())
}
