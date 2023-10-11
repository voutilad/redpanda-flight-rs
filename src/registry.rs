use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard};

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{ClientConfig, ClientContext, Message};

use crate::schema::{RedpandaSchema, Schema};

struct RegistryContext;
impl ClientContext for RegistryContext {}
impl ConsumerContext for RegistryContext {}

pub struct Registry {
    topic: String,
    map: RwLock<HashMap<String, Schema>>,
    consumer: BaseConsumer<RegistryContext>,
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
        let consumer: BaseConsumer<RegistryContext> =
            match base_config.create_with_context(RegistryContext {}) {
                Ok(c) => c,
                Err(e) => return Err(e.to_string()),
            };

        if consumer.subscribe(&[topic]).is_err() {
            Err(String::from("failed to subscribe to schema registry topic"))
        } else {
            Ok(Registry {
                topic: String::from(topic),
                map: RwLock::new(HashMap::new()),
                consumer,
            })
        }
    }

    pub fn hydrate(&self) -> Result<usize, String> {
        let mut map = self.map.write().unwrap();
        let mut cnt: usize = 0;

        for result in self.consumer.iter() {
            let message = match result {
                Ok(m) => m,
                Err(_) => return Err(String::from("no message")),
            };
            let value: RedpandaSchema =
                serde_json::from_slice(message.payload().unwrap_or(&[])).unwrap();
            let schema = Schema::from(value).unwrap();
            map.insert(schema.topic.clone(), schema);
            cnt += 1;
        }
        Ok(cnt)
    }

    pub fn lookup(&self, topic: &str) -> Option<RwLockReadGuard<HashMap<String, Schema>>> {
        let map = self.map.read().unwrap();
        if map.contains_key(topic) {
            Some(map)
        } else {
            None
        }
    }
}
