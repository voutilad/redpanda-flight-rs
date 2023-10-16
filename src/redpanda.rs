use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info};

struct RedpandaContext;
impl ClientContext for RedpandaContext {}
impl ConsumerContext for RedpandaContext {}

const DEFAULT_BATCH_SIZE: usize = 100;
static STREAM_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Represents information about a Topic-Partition in Redpanda.
/// N.b. some of these types are signed because of RDKafka & Java :(
#[derive(Debug)]
pub struct TopicPartition {
    /// Name of the topic.
    pub topic: String,
    /// Partition id.
    pub id: i32,
    /// Low and High watermarks (offsets) for the partition. Nb. May not form a contiguous range!
    pub watermarks: (i64, i64),
    /// Approximate size of the partition (on disk).
    pub bytes: usize,
}

/// Represents information about a Topic in Redpanda.
pub struct Topic {
    pub topic: String,
    pub partitions: Vec<TopicPartition>,
}

pub struct BatchingStream {
    pub batch_size: usize,

    /// Estimate of how many messages remain in the stream. May not be accurate due to compaction.
    pub remainder: AtomicUsize,

    /// Target offset determining the end of the stream for a topic partition.
    pub target_offset: i64,

    _permit: OwnedSemaphorePermit,

    consumer: StreamConsumer<RedpandaContext>,

    /// A simple identifier for this stream, for now.
    stream_id: usize,

    last_offset: i64,
}

impl BatchingStream {
    /// Consume the next n items.
    pub async fn next_batch(&self) -> Result<Option<Vec<OwnedMessage>>, String> {
        // Most likely reason to bail early is we're past our expected watermark.
        if self.last_offset >= self.target_offset {
            debug!("stream {} consumed; past watermark", self.stream_id);
            return Ok(None);
        }

        // There's a chance we hit our expected limit. Not guaranteed, though.
        if self.remainder.load(Ordering::Relaxed) == 0 {
            debug!("stream {} consumed; no messages remaining", self.stream_id);
            return Ok(None);
        }

        let mut v: Vec<OwnedMessage> = Vec::with_capacity(self.batch_size);
        let mut stream = self.consumer.stream();
        loop {
            let msg = match stream.next().await {
                None => return Ok(None),
                Some(r) => match r {
                    Ok(m) => m.detach(),
                    Err(e) => {
                        error!("kafka error consuming stream: {}", e);
                        return Err(e.to_string());
                    }
                },
            };
            let offset = msg.offset();
            v.push(msg);
            debug!("added message with offset {}", offset);

            self.remainder.fetch_sub(1, Ordering::Relaxed);
            if offset >= self.target_offset {
                break;
            }
            if v.len() == self.batch_size {
                break;
            }
        }

        Ok(Some(v))
    }
}

/// Redpanda service abstraction.
pub struct Redpanda {
    pub seeds: String,
    metadata_client: BaseConsumer<RedpandaContext>,
    stream_permits: Arc<Semaphore>,
}

impl Redpanda {
    /// Initialize a Redpanda connection, establishing the metadata client.
    pub fn connect(seeds: &str) -> Result<Redpanda, String> {
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
                    stream_permits: Arc::new(Semaphore::new(10)), // TODO: arbitrary
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
                let partitions: Vec<TopicPartition> = topic
                    .partitions()
                    .iter()
                    .map(|p| {
                        // XXX TODO: this call is blocking
                        let (low, high) = self
                            .metadata_client
                            .fetch_watermarks(name, p.id(), Timeout::After(Duration::from_secs(5)))
                            .unwrap_or((0, 0));
                        TopicPartition {
                            topic: String::from(name),
                            id: p.id(),
                            watermarks: (low, high),
                            bytes: 0, // TODO: need to use that api call for log dir size
                        }
                    })
                    .collect();
                Topic {
                    topic: String::from(name),
                    partitions,
                }
            })
            .collect();
        Ok(result)
    }

    /// Fetch information on a particular topic partition (i.e. watermarks).
    pub fn get_topic_partition(&self, topic: &str, pid: i32) -> Result<TopicPartition, String> {
        // XXX TODO: this is blocking!
        let metadata = match self
            .metadata_client
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))
        {
            Ok(m) => m,
            Err(e) => return Err(e.to_string()),
        };
        if metadata.topics().len() != 1 {
            error!(
                "bad metadata response, expected 1 topic but got {}",
                metadata.topics().len()
            );
            return Err(String::from("unexpected metadata response"));
        }
        let topic_meta = metadata.topics().first().unwrap();
        let partition = match topic_meta.partitions().iter().find(|&p| p.id() == pid) {
            None => {
                error!("failed to find partition {} for topic {}", pid, topic);
                return Err(String::from("cannot find partition for topic"));
            }
            Some(p) => p,
        };
        // XXX TODO: this is blocking?!
        let watermarks = match self.metadata_client.fetch_watermarks(
            topic,
            pid,
            Timeout::After(Duration::from_secs(5)),
        ) {
            Ok(w) => w,
            Err(e) => {
                error!(
                    "failed to fetch watermarks for partition {} of topic {}",
                    pid, topic
                );
                return Err(e.to_string());
            }
        };

        let result = TopicPartition {
            topic: String::from(topic),
            id: partition.id(),
            watermarks,
            bytes: 0,
        };
        Ok(result)
    }

    /// Generate a bounded stream from a topic partition.
    pub async fn stream(&self, tp: &TopicPartition) -> Result<BatchingStream, String> {
        let permit = match self.stream_permits.clone().acquire_owned().await {
            Ok(p) => p,
            Err(e) => return Err(e.to_string()),
        };
        let stream_id = STREAM_COUNTER.fetch_add(1, Ordering::Relaxed);

        // TODO: batching goes here?
        // TODO: max.poll.records or something?
        let base_config: ClientConfig = ClientConfig::new()
            .set("bootstrap.servers", self.seeds.clone())
            .set("group.id", format!("redpanda-flight-stream-{}", stream_id))
            .set_log_level(RDKafkaLogLevel::Warning)
            .clone();
        let consumer: StreamConsumer<RedpandaContext> =
            match base_config.create_with_context(RedpandaContext {}) {
                Ok(c) => c,
                Err(e) => {
                    error!("error creating StreamConsumer: {}", e);
                    return Err(e.to_string());
                }
            };

        // TODO: this is blocking...needs asyncification
        // Assign a topic partition to this consumer.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(tp.topic.as_str(), tp.id);
        tpl.set_partition_offset(tp.topic.as_str(), tp.id, Offset::Beginning)
            .unwrap();
        match consumer.assign(&tpl) {
            Ok(_) => {}
            Err(e) => {
                error!("error assigning TopicPartitionList {:?}", tpl);
                return Err(e.to_string());
            }
        };

        debug!(
            "creating BatchingStream for topic partition {}/{}, target watermark {}, and id {}",
            tp.topic, tp.id, tp.watermarks.1, stream_id
        );
        Ok(BatchingStream {
            batch_size: DEFAULT_BATCH_SIZE, // TODO: configure
            remainder: AtomicUsize::new((tp.watermarks.1 - tp.watermarks.0) as usize),
            target_offset: tp.watermarks.1 - 1, // XXX Redpanda reports offset + 1!
            _permit: permit,
            consumer,
            stream_id,
            last_offset: 0,
        })
    }
}
