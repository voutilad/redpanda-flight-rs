use std::env;
use std::path::Path;
use std::process::ExitCode;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{error, info, warn};
use tracing_subscriber;

use crate::redpanda::{Auth, AuthMechanism, AuthProtocol};

mod convert;
mod redpanda;
mod registry;
mod schema;
mod server;

// These are borrowed from Redpanda's docs to try to simplify alignment.
// See https://docs.redpanda.com/current/manage/security/encryption/
const DEFAULT_BROKER_KEY: &str = "broker.key";
const DEFAULT_BROKER_CERT: &str = "broker.crt";

#[tokio::main]
async fn main() -> ExitCode {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    let seeds = env::var("REDPANDA_BROKERS").unwrap_or(String::from("localhost:9092"));
    let topic = env::var("REDPANDA_SCHEMA_TOPIC").unwrap_or(String::from("_schemas"));
    info!("using Schema Registry topic {} via seed {}", topic, seeds);

    let env_username = env::var("REDPANDA_SASL_USERNAME");
    let env_password = env::var("REDPANDA_SASL_PASSWORD");
    let env_mechanism = env::var("REDPANDA_SASL_MECHANISM");
    let use_tls = env::var("REDPANDA_TLS_ENABLED").is_ok();
    let env_cert = env::var("REDPANDA_FLIGHT_CERT").unwrap_or(String::from(DEFAULT_BROKER_CERT));
    let env_key = env::var("REDPANDA_FLIGHT_KEY").unwrap_or(String::from(DEFAULT_BROKER_KEY));
    let tls_cert = Path::new(&env_cert);
    let tls_key = Path::new(&env_key);

    // Piece together our authentication details for our backend/admin connection.
    let auth: Option<Auth> = match env_username {
        Ok(username) => {
            if env_password.is_err() {
                None
            } else {
                let password = env_password.unwrap();
                match env_mechanism {
                    Ok(m) => {
                        let mechanism = AuthMechanism::from_string(&m);
                        if use_tls {
                            let protocol = match mechanism {
                                AuthMechanism::Plain => AuthProtocol::Ssl,
                                _ => AuthProtocol::SaslSsl,
                            };
                            Some(Auth {
                                username,
                                password,
                                protocol,
                                mechanism,
                            })
                        } else {
                            let protocol = match mechanism {
                                AuthMechanism::Plain => AuthProtocol::Plaintext,
                                _ => AuthProtocol::SaslPlain,
                            };
                            Some(Auth {
                                username,
                                password,
                                protocol,
                                mechanism,
                            })
                        }
                    }
                    Err(_) => Some(Auth {
                        username,
                        password,
                        protocol: AuthProtocol::Plaintext,
                        mechanism: AuthMechanism::Plain,
                    }),
                }
            }
        }
        Err(_) => None,
    };
    if auth.is_some() {
        info!("using authentication: {}", auth.as_ref().unwrap());
    } else {
        warn!("not using authentication to talk to Redpanda");
    }

    let addr = env::var("REDPANDA_FLIGHT_ADDR")
        .unwrap_or(String::from("127.0.0.1:9999"))
        .parse()
        .unwrap();
    info!("listening on {}", addr);

    // XXX We require TLS on the front-end if we're using it on the back-end to prevent security
    // foot-guns. To prevent the illusion of security, we expect both sides to be using TLS or not.
    let tls_config: Option<ServerTlsConfig> = match use_tls {
        true => {
            // TODO: graceful error handling
            let cert = tokio::fs::read(tls_cert).await.unwrap();
            let key = tokio::fs::read(tls_key).await.unwrap();
            Some(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
        }
        false => {
            if auth.is_some() {
                warn!("authentication provided, but no TLS configured for front-end");
                return ExitCode::FAILURE;
            }
            None
        }
    };

    let redpanda = server::RedpandaFlightService::new(seeds.as_str(), topic.as_str(), auth).await;
    if redpanda.is_err() {
        return ExitCode::FAILURE;
    }
    let svc = FlightServiceServer::new(redpanda.unwrap());

    // Launch the flight service.
    let mut builder = Server::builder();
    if tls_config.is_some() {
        builder = match builder.tls_config(tls_config.unwrap()) {
            Ok(s) => {
                info!("configured front-end for TLS");
                s
            }
            Err(e) => {
                error!("failed to initialize tls config: {}", e);
                return ExitCode::FAILURE;
            }
        };
    }
    if builder.add_service(svc).serve(addr).await.is_err() {
        return ExitCode::FAILURE;
    }

    info!("bye!");
    ExitCode::SUCCESS
}
