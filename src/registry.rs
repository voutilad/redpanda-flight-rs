use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::schema::{RedpandaSchema, Schema};
use futures::FutureExt;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, ConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::{JoinError, JoinHandle};

struct RegistryContext;
impl ClientContext for RegistryContext {}
impl ConsumerContext for RegistryContext {}

pub struct Registry {
    topic: String,
    map: Arc<RwLock<HashMap<String, Schema>>>,
}

impl Registry {
    pub fn new(topic: &str, seeds: &str) -> Result<Registry, String> {
        /// Create a new Registry instance.
        let base_config: ClientConfig = ClientConfig::new()
            .set("group.id", "redpanda-flight-registry")
            .set("bootstrap.servers", seeds)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Warning)
            .clone();
        let consumer: StreamConsumer<RegistryContext> =
            match base_config.create_with_context(RegistryContext {}) {
                Ok(c) => c,
                Err(e) => return Err(e.to_string()),
            };

        // We don't use subscription mode as that creates consumer group behavior.
        let mut tpl = TopicPartitionList::default();
        tpl.add_partition(topic, 0);
        if consumer.assign(&tpl).is_err() {
            return Err(String::from("failed to assign topic partition to consumer"));
        }

        let map = Arc::new(RwLock::new(HashMap::new()));
        let registry = Registry {
            topic: String::from(topic),
            map: map.clone(),
        };
        let f = tokio::spawn(async move { Registry::hydrate(consumer, map).await });
        Ok(registry)
    }

    async fn hydrate(
        consumer: StreamConsumer<RegistryContext>,
        map: Arc<RwLock<HashMap<String, Schema>>>,
    ) -> Result<(), String> {
        /// Update the view of the Schema Registry.
        ///
        let mut stream = consumer.stream();
        loop {
            let message = match stream.next().await {
                None => break,
                Some(r) => match r {
                    Ok(m) => m,
                    Err(_) => return Err(String::from("failure")),
                },
            };
            let value: RedpandaSchema =
                serde_json::from_slice(message.payload().unwrap_or(&[])).unwrap();
            let schema = Schema::from(value).unwrap();

            // Grab the write lock and insert.
            let mut map = map.write().await;
            map.insert(schema.topic.clone(), schema);
        }
        Ok(())
    }

    pub async fn lookup(&self, topic: &str) -> Option<RwLockReadGuard<HashMap<String, Schema>>> {
        let map = self.map.read().await;
        if map.contains_key(topic) {
            Some(map)
        } else {
            None
        }
    }
}
