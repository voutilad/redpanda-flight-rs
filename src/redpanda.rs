use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext};
use std::time::Duration;
use tracing::info;

struct RedpandaContext;
impl ClientContext for RedpandaContext {}
impl ConsumerContext for RedpandaContext {}

pub struct Redpanda {
    pub seeds: String,
    metadata_client: BaseConsumer<RedpandaContext>,
}

impl Redpanda {
    pub fn connect(seeds: &str) -> Result<Redpanda, String> {
        /// Initialize a Redpanda connection, establishing the metadata client.
        let base_config: ClientConfig = ClientConfig::new()
            .set("bootstrap.servers", seeds)
            .set_log_level(RDKafkaLogLevel::Warning)
            .clone();
        let metadata_client: BaseConsumer<RedpandaContext> =
            match base_config.create_with_context(RedpandaContext {}) {
                Ok(c) => c,
                Err(e) => return Err(e.to_string()),
            };

        // TODO: The BaseConsumer isn't async. Need to revisit this.
        // Fetch the cluster id to check our connection.
        match metadata_client
            .client()
            .fetch_cluster_id(Timeout::After(Duration::from_millis(5000)))
        {
            None => Err(String::from("timed out connecting to Redpanda")),
            Some(id) => {
                info!("connected to Redpanda cluster {}", id);
                Ok(Redpanda {
                    seeds: String::from(seeds),
                    metadata_client,
                })
            }
        }
    }
}
