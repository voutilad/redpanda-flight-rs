use std::env;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber;

mod convert;
mod redpanda;
mod registry;
mod schema;
mod server;

#[tokio::main]
async fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    let seeds = env::var("REDPANDA_BROKERS").unwrap_or(String::from("localhost:9092"));
    let topic = env::var("REDPANDA_SCHEMA_TOPIC").unwrap_or(String::from("_schemas"));

    let addr = "127.0.0.1:9999".parse().unwrap();
    info!("starting with address {}", addr);

    let redpanda = match server::RedpandaFlightService::new(seeds.as_str(), topic.as_str()).await {
        Ok(r) => r,
        Err(e) => {
            error!("failure to launch: {}", e);
            return;
        }
    };
    let svc = FlightServiceServer::new(redpanda);

    match Server::builder().add_service(svc).serve(addr).await {
        Ok(_) => {}
        Err(e) => {
            error!("failure to start flight service: {}", e);
            return;
        }
    };

    info!("bye!");
}
