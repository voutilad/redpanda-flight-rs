use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use serde::Deserialize;
use tokio::sync::RwLock;
use tokio::task;
use tracing::{error, info, warn};

use crate::redpanda::Auth;
use crate::schema::Schema;

struct RegistryContext;

impl ClientContext for RegistryContext {}

impl ConsumerContext for RegistryContext {}

/// A client for interacting with the Redpanda Schema Registry via the internal topic (i.e. via the Kafka API).
/// Maintains a global view of all known [Schema](Schema)
pub struct Registry {
    pub topic: String,
    map: Arc<RwLock<HashMap<String, Schema>>>,
}

/// Represents an entry in the Redpanda Schema Registry as seen from the underlying topic.
#[derive(Deserialize, Debug)]
pub struct SchemaRegistryEntry {
    pub subject: String,
    pub version: i64,
    pub id: i64,
    pub schema: String,
}

impl Registry {
    /// Create a new Registry instance.
    pub async fn new(
        topic: &str,
        seeds: &str,
        admin_auth: &Option<Auth>,
    ) -> Result<Registry, String> {
        let mut base_config: ClientConfig = ClientConfig::new()
            .set("group.id", "redpanda-flight-registry")
            .set("bootstrap.servers", seeds)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Warning)
            .clone();

        if admin_auth.is_some() {
            let auth = admin_auth.as_ref().unwrap();
            base_config = base_config
                .set("sasl.username", &auth.username)
                .set("sasl.password", &auth.password)
                .set("security.protocol", auth.protocol.as_str())
                .set("sasl.mechanisms", auth.mechanism.as_str())
                .clone();
        }

        // We don't use subscription mode as that creates consumer group behavior.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, 0);
        tpl.set_partition_offset(topic, 0, Offset::Beginning)
            .unwrap();

        // Create our consumer by spawning a blocking task. The initial partition assignment
        // can cause a Kafka API request to a broker, so it's considered blocking.
        //
        let future = task::spawn_blocking(move || {
            let consumer: StreamConsumer<RegistryContext> =
                match base_config.create_with_context(RegistryContext {}) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("failed to create a Registry consumer: {}", e);
                        return None;
                    }
                };

            // XXX This is the blocking call.
            match consumer.assign(&tpl) {
                Ok(_) => Some(consumer),
                Err(e) => {
                    error!("error assigning TopicPartitionList {:?}: {}", tpl, e);
                    return None;
                }
            }
        })
        .await;

        let consumer = match future {
            Ok(c) => match c {
                None => return Err(String::from("failed to create consumer")),
                Some(c) => c,
            },
            Err(e) => {
                return Err(String::from(format!(
                    "unexpected failure creating consumer: {}",
                    e
                )));
            }
        };

        let map = Arc::new(RwLock::new(HashMap::new()));
        let registry = Registry {
            topic: String::from(topic),
            map: map.clone(),
        };

        tokio::spawn(async move { Registry::hydrate(consumer, map).await });
        Ok(registry)
    }

    /// Update the view of the Schema Registry.
    async fn hydrate(
        consumer: StreamConsumer<RegistryContext>,
        map: Arc<RwLock<HashMap<String, Schema>>>,
    ) -> Result<(), String> {
        let mut stream = consumer.stream();
        info!("hydrating");
        loop {
            let message = match stream.next().await {
                None => break,
                Some(r) => match r {
                    Ok(m) => m,
                    Err(_) => return Err(String::from("unexpected stream failure")),
                },
            };
            let value: SchemaRegistryEntry =
                match serde_json::from_slice(message.payload().unwrap_or(&[])) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("deserialization error during hydration: {}", e);
                        continue; // Skip this entry.
                    }
                };
            let result = Schema::from(&value);
            if result.is_err() {
                warn!("can't parse schema for subject {}", value.subject);
                continue; // Skip this entry.
            }
            let schema = result.unwrap();
            info!(
                "hydrated a schema for {} (id={}, version={})",
                schema.topic, schema.id, schema.version
            );

            // Grab the write lock and insert.
            let mut map = map.write().await;
            map.insert(schema.topic.clone(), schema);
            drop(map);
        }
        Ok(())
    }

    pub async fn lookup(&self, topic: &str) -> Option<Schema> {
        let map = self.map.read().await;
        match map.get(topic) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }
}
