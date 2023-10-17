use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::metadata::Metadata;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task;
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
    metadata_client: Arc<BaseConsumer<RedpandaContext>>,
    stream_permits: Arc<Semaphore>,
}

impl Redpanda {
    /// Initialize a Redpanda connection, establishing the metadata client.
    pub async fn connect(seeds: &str) -> Result<Redpanda, String> {
        let base_config: ClientConfig = ClientConfig::new()
            .set("bootstrap.servers", seeds)
            .set_log_level(RDKafkaLogLevel::Warning)
            .clone();
        let metadata_client: BaseConsumer<RedpandaContext> =
            match base_config.create_with_context(RedpandaContext {}) {
                Ok(c) => c,
                Err(e) => return Err(e.to_string()),
            };

        // Fetch the cluster id to check our connection.
        let _seeds = String::from(seeds);
        let result = task::spawn_blocking(move || {
            match metadata_client
                .client()
                .fetch_cluster_id(Timeout::After(Duration::from_millis(5000)))
            {
                None => Err(String::from("timed out connecting to Redpanda")),
                Some(id) => {
                    info!("connected to Redpanda cluster {}", id);
                    Ok(Redpanda {
                        seeds: _seeds,
                        metadata_client: Arc::new(metadata_client),
                        stream_permits: Arc::new(Semaphore::new(10)), // TODO: arbitrary
                    })
                }
            }
        })
        .await;
        match result {
            Ok(r) => r,
            Err(e) => {
                error!("failed to connect to Redpanda cluster");
                return Err(e.to_string());
            }
        }
    }

    pub async fn list_topics(&self) -> Result<Vec<Topic>, String> {
        // XXX Bit of a TOCTOU here getting topics and then getting watermarks.
        let metadata = match self.fetch_metadata(None).await {
            Ok(m) => m,
            Err(e) => return Err(e.to_string()),
        };
        let mut topics: Vec<Topic> = Vec::new();

        // Build Copy-able state to navigate asyncifying the sync stuff.
        let pairs: Vec<(String, Vec<i32>)> = metadata
            .topics()
            .iter()
            .map(|t| {
                let topic = String::from(t.name());
                let pids = t.partitions().iter().map(|p| p.id()).collect();
                (topic, pids)
            })
            .collect();

        for (t, pids) in pairs {
            let mut tps: Vec<TopicPartition> = Vec::new();

            for pid in pids {
                let watermarks = match self.fetch_watermarks(t.as_str(), pid).await {
                    Ok(w) => w,
                    Err(e) => {
                        error!("failed to list topics: {}", e);
                        return Err(e);
                    }
                };
                tps.push(TopicPartition {
                    topic: t.clone(),
                    id: pid,
                    watermarks,
                    bytes: 0, // XXX TODO: needs an extra api call to get bytes
                });
            }
            topics.push(Topic {
                topic: String::from(t.clone()),
                partitions: tps,
            });
        }

        Ok(topics)
    }

    pub async fn fetch_metadata(&self, topics: Option<&str>) -> Result<Metadata, String> {
        // Do a silly async dance...
        let s = String::from(topics.unwrap_or(""));
        let _client = self.metadata_client.clone();

        match task::spawn_blocking(move || {
            // ...continue the silly async dance.
            let _s = s.clone();
            let _topics: Option<&str>;
            if _s.is_empty() {
                _topics = None;
            } else {
                _topics = Some(_s.as_str());
            }
            match _client.fetch_metadata(_topics, Timeout::After(Duration::from_secs(5))) {
                Ok(m) => Ok(m),
                Err(e) => Err(e.to_string()),
            }
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("failed to fetch metadata: {}", e.to_string());
                return Err(e.to_string());
            }
        }
    }

    pub async fn fetch_watermarks(&self, topic: &str, pid: i32) -> Result<(i64, i64), String> {
        // Do a silly async dance...
        let _client = self.metadata_client.clone();
        let _topic = String::from(topic);

        match task::spawn_blocking(move || {
            match _client.fetch_watermarks(
                _topic.as_str(),
                pid,
                Timeout::After(Duration::from_secs(5)),
            ) {
                Ok(w) => Ok(w),
                Err(e) => Err(e.to_string()),
            }
        })
        .await
        {
            Ok(r) => r,
            Err(e) => Err(String::from(format!(
                "failed to fetch watermarks for topic partition {}/{}: {}",
                topic,
                pid,
                e.to_string()
            ))),
        }
    }

    /// Fetch information on a particular topic partition (i.e. watermarks).
    pub async fn get_topic_partition(
        &self,
        topic: &str,
        pid: i32,
    ) -> Result<TopicPartition, String> {
        let metadata = match self.fetch_metadata(Some(topic)).await {
            Ok(m) => m,
            Err(e) => return Err(e),
        };

        // Validate metadata and find our partition by id.
        if metadata.topics().len() != 1 {
            error!(
                "bad metadata response, expected 1 topic but got {}",
                metadata.topics().len()
            );
            return Err(String::from("unexpected metadata response"));
        }
        match metadata
            .topics()
            .first()
            .unwrap()
            .partitions()
            .iter()
            .find(|&p| p.id() == pid)
        {
            None => {
                error!("failed to find partition {} for topic {}", pid, topic);
                return Err(String::from("cannot find partition for topic"));
            }
            Some(p) => p,
        };

        let watermarks = match self.fetch_watermarks(topic, pid).await {
            Ok(w) => w,
            Err(e) => return Err(e),
        };

        Ok(TopicPartition {
            topic: String::from(topic),
            id: pid,
            watermarks,
            bytes: 0,
        })
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
