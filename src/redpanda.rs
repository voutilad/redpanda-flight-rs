use arrow::array::Array;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext};
use std::time::Duration;
use tracing::info;

struct RedpandaContext;
impl ClientContext for RedpandaContext {}
impl ConsumerContext for RedpandaContext {}

/// Redpanda service abstraction.
pub struct Redpanda {
    pub seeds: String,
    metadata_client: BaseConsumer<RedpandaContext>,
}

/// Represent information about a Topic in Redpanda.
pub struct Topic {
    pub topic: String,
    pub partitions: Vec<i32>,
    pub messages: u32,
    pub bytes: usize,
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

    pub fn list_topics(&self) -> Result<Vec<Topic>, String> {
        // XXX Bit of a TOCTOU here getting topics and then getting watermarks.
        let metadata = match self
            .metadata_client
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
        {
            Ok(m) => m,
            Err(e) => return Err(e.to_string()),
        };
        let result: Vec<Topic> = metadata
            .topics()
            .iter()
            .map(|topic| {
                let name = topic.name();
                let cnt: i64 = topic
                    .partitions()
                    .iter()
                    .map(|partition| {
                        /// TODO: better error handling
                        let (hi, lo) = self
                            .metadata_client
                            .fetch_watermarks(
                                name,
                                partition.id(),
                                Timeout::After(Duration::from_secs(5)),
                            )
                            .unwrap();
                        // Return the delta
                        hi - lo
                    })
                    .sum();
                Topic {
                    topic: String::from(name),
                    partitions: topic.partitions().iter().map(|p| p.id()).collect(),
                    messages: cnt as u32, // XXX TODO: shitty cast
                    bytes: 0, // TODO: we need to use that admin api call to get log dir sizes
                }
            })
            .collect();
        Ok(result)
    }
}
