use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use futures::{FutureExt, Stream, StreamExt};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::metadata::Metadata;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task;
use tracing::{debug, error, info};

use crate::convert::convert;
use crate::schema::Schema;

struct RedpandaContext;

impl ClientContext for RedpandaContext {}

impl ConsumerContext for RedpandaContext {}

const DEFAULT_BATCH_SIZE: usize = 1_000;
static STREAM_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Represents information about a Topic-Partition in Redpanda.
/// N.b. some of these types are signed because of RDKafka & Java :(
#[derive(Debug, Clone)]
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
    last_offset: i64,
    schema: Schema,
    stream_id: usize,
    consumer: Pin<Box<StreamConsumer<RedpandaContext>>>,
}

impl BatchingStream {
    fn new(
        permit: OwnedSemaphorePermit,
        schema: Schema,
        consumer: StreamConsumer<RedpandaContext>,
        target_offset: i64,
        remainder: usize,
        stream_id: usize,
    ) -> BatchingStream {
        BatchingStream {
            batch_size: DEFAULT_BATCH_SIZE,
            remainder: AtomicUsize::new(remainder),
            target_offset,
            _permit: permit,
            last_offset: 0,
            schema,
            stream_id,
            consumer: Box::pin(consumer),
        }
    }
}

impl Stream for BatchingStream {
    type Item = Result<RecordBatch, FlightError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Most likely reason to bail early is we're past our expected watermark.
        if self.last_offset >= self.target_offset {
            debug!("stream {} consumed; past watermark", self.stream_id);
            return Poll::Ready(None);
        }

        // There's a chance we hit our expected limit. Not guaranteed, though.
        if self.remainder.load(Ordering::Relaxed) == 0 {
            debug!("stream {} consumed; no messages remaining", self.stream_id);
            return Poll::Ready(None);
        }

        let mut batch: Vec<OwnedMessage> = Vec::with_capacity(self.batch_size);

        // Build up our batch
        let mut stream = self.consumer.stream();
        debug!("created MessageStream");
        loop {
            let result = match stream.next().poll_unpin(cx) {
                Poll::Ready(m) => m,
                Poll::Pending => None,
            };
            if result.is_none() {
                continue;
            }
            let message = match result.unwrap() {
                Ok(m) => m.detach(),
                Err(e) => {
                    error!("error polling next Kafka message: {}", e);
                    return Poll::Ready(Some(Err(FlightError::ExternalError(Box::new(e)))));
                }
            };

            let offset = message.offset();
            batch.push(message);

            self.remainder.fetch_sub(1, Ordering::Relaxed);
            if offset >= self.target_offset {
                break;
            }
            if batch.len() == self.batch_size {
                break;
            }
        }
        drop(stream);
        debug!("built batch of {} messages", batch.len());

        if batch.is_empty() {
            debug!("stream done?");
            return Poll::Ready(None);
        }
        let rb = match convert(&batch, &self.schema) {
            Ok(rb) => rb,
            Err(e) => {
                debug!("stream failed: {}", e);
                return Poll::Ready(Some(Err(FlightError::DecodeError(e)))); // TODO: use a better error that's semantically valid
            }
        };

        debug!("emitting a record batch");
        Poll::Ready(Some(Ok(rb)))
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

    /// List details on all [Topics](redpanda::Topic) for a Redpanda cluster.
    pub async fn list_topics(&self) -> Result<Vec<Topic>, String> {
        // XXX Bit of a TOCTOU here getting topics and then getting watermarks.
        let metadata = match self.fetch_metadata(None).await {
            Ok(m) => m,
            Err(e) => return Err(e.to_string()),
        };
        let mut topics: Vec<Topic> = Vec::new();

        // Build Copy-able state to make async the sync stuff in a simple way without a Pin nightmare.
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

    /// Fetch metadata for a topic or topics (if None specified). Used to get partition information.
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

    /// Asynchronously fetch the watermark information (low, high) for a topic partition.
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

    /// Generate a bounded [BatchingStream] from a [TopicPartition].
    pub async fn stream(
        &self,
        tp: &TopicPartition,
        schema: &Schema,
    ) -> Result<BatchingStream, String> {
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

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(tp.topic.as_str(), tp.id);
        tpl.set_partition_offset(tp.topic.as_str(), tp.id, Offset::Beginning)
            .unwrap();

        // TODO: this is blocking...needs asyncification
        // Assign a topic partition to this consumer.

        // Create our consumer by spawning a blocking task. The initial partition assignment
        // can cause a Kafka API request to a broker, so it's considered blocking.
        //
        let future = task::spawn_blocking(move || {
            let consumer: StreamConsumer<RedpandaContext> =
                match base_config.create_with_context(RedpandaContext {}) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("error creating StreamConsumer: {}", e);
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
            Err(_) => return Err(String::from("unexpected error creating consumer")),
        };

        debug!(
            "creating BatchingStream for topic partition {}/{}, target watermark {}",
            tp.topic, tp.id, tp.watermarks.1
        );

        Ok(BatchingStream::new(
            permit,
            schema.clone(),
            consumer,
            tp.watermarks.1 - 1,
            (tp.watermarks.1 - tp.watermarks.0) as usize,
            stream_id,
        ))
    }
}
