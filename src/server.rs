use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_stream::stream;
use futures::stream;
use futures::stream::{BoxStream, StreamExt};
use rdkafka::message::OwnedMessage;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, warn};

use crate::redpanda::{BatchingStream, Redpanda, Topic, TopicPartition};
use crate::registry::Registry;
use crate::schema::Schema;

/// Used to join a topic name and a partition id to form a ticket. By choice, this separator
/// contains value(s) not valid for Apache Kafka Topics.
const TICKET_SEPARATOR: &str = "/";

pub struct RedpandaFlightService {
    pub redpanda: Redpanda,
    pub registry: Registry,
    pub seeds: String,
}

impl RedpandaFlightService {
    pub fn new(seeds: &str, schemas_topic: &str) -> RedpandaFlightService {
        RedpandaFlightService {
            redpanda: Redpanda::connect(seeds).unwrap(),
            registry: Registry::new(schemas_topic, seeds).unwrap(),
            seeds: String::from(seeds),
        }
    }
}

#[tonic::async_trait]
impl FlightService for RedpandaFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        warn!("handshake not implemented");
        Err(Status::unimplemented("Implement handshake"))
    }
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // let vec: Vec<Result<FlightInfo, Status>> = Vec::new();

        let topics: Vec<Topic> = match self.redpanda.list_topics() {
            Ok(v) => v,
            Err(e) => return Err(Status::internal(e)), // XXX fail hard for now
        };
        let mut results: Vec<Result<FlightInfo, Status>> = Vec::with_capacity(topics.len());

        for t in topics {
            let schema = match self.registry.lookup(t.topic.as_str()).await {
                Some(s) => s,
                None => {
                    warn!("topic {} missing value schema", t.topic);
                    continue;
                }
            };
            let desc = FlightDescriptor::new_path(vec![t.topic.clone()]);

            // TODO: tie into service Location
            let result = match FlightInfo::new().try_with_schema(&schema.schema_arrow) {
                Ok(mut info) => {
                    info = info
                        .with_descriptor(desc)
                        .with_ordered(true)
                        .with_total_records(
                            t.partitions
                                .iter()
                                .map(|p| p.watermarks.1 - p.watermarks.0)
                                .sum(),
                        );
                    for tp in t.partitions {
                        info = info.with_endpoint(
                            FlightEndpoint::new()
                                .with_location("grpc+tcp://localhost:9999")
                                .with_ticket(Ticket::new(String::from_iter([
                                    t.topic.clone().as_str(),
                                    TICKET_SEPARATOR,
                                    tp.id.to_string().as_str(),
                                ]))),
                        )
                    }
                    Ok(info)
                }
                Err(e) => Err(Status::internal(e.to_string())),
            };
            results.push(result);
        }

        let stream = futures::stream::iter(results);
        Ok(Response::new(stream.boxed()))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        warn!("get_flight_info not implemented");
        Err(Status::unimplemented("Implement get_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let req = _request.get_ref();
        if req.r#type() == DescriptorType::Cmd {
            return Err(Status::invalid_argument(
                "only PATH type descriptors are supported",
            ));
        }
        if req.path.len() > 1 {
            return Err(Status::invalid_argument("invalid path format"));
        }

        let empty = String::new();
        let topic = req.path.first().unwrap_or(&empty);
        let result = self.registry.lookup(topic.as_str()).await;
        let schema = match result {
            None => return Err(Status::not_found("no schema for topic")),
            Some(s) => s,
        };
        let info = FlightInfo::new()
            .try_with_schema(&schema.schema_arrow)
            .unwrap();
        Ok(Response::new(SchemaResult {
            schema: info.schema,
        }))
    }
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Decompose ticket
        // N.b. Apache Kafka topics are ascii.
        // See Apache Kafka's clients/src/main/java/org/apache/kafka/common/internals/Topic.java
        let ticket = request.into_inner().ticket;
        if !ticket.is_ascii() {
            return Err(Status::failed_precondition(String::from(
                "redpanda-flight tickets must be ascii",
            )));
        }
        if !ticket.contains(
            TICKET_SEPARATOR
                .as_bytes()
                .first()
                .expect("ticket separator does not contain ascii??!"),
        ) {
            return Err(Status::failed_precondition(String::from(
                "unintelligible redpanda-flight ticket",
            )));
        }

        let parts: Vec<String> = ticket
            .rsplitn(2, |c| c.to_string() == TICKET_SEPARATOR)
            .map(|part| {
                String::from_utf8(Vec::from(part))
                    .expect("non utf-8 char in ticket even though we checked is_ascii?!")
            })
            .collect();
        if parts.len() != 2 {
            return Err(Status::internal("something went wrong in ticket parsing"));
        }
        let topic = match parts.first() {
            None => {
                return Err(Status::failed_precondition(
                    "bad topic in redpanda-flight ticket",
                ))
            }
            Some(t) => t,
        };
        let pid = match i32::from_str(parts.last().unwrap_or(&String::from("")).as_str()) {
            Ok(pid) => pid,
            Err(e) => {
                return Err(Status::failed_precondition(
                    "partition id in ticket must be numeric",
                ))
            }
        };

        let schema = match self.registry.lookup(topic.as_str()).await {
            None => return Err(Status::not_found("no matching schema found")),
            Some(s) => s,
        };

        // TODO: do we need a cache of watermarks? or just do another lookup via the kafka api?
        let tp = TopicPartition::new_from(topic.as_str(), pid);

        let batches = match self.redpanda.stream(&tp).await {
            Ok(s) => s,
            Err(e) => {
                error!("failed to create stream for {:?}", tp);
                return Err(Status::internal(e));
            }
        };
        // TODO: we need to build and return an actual stream lest we block!
        let mut results: Vec<RecordBatch> = Vec::new();
        loop {
            // Consume data for now, but drop it.
            match batches.next_batch().await {
                Ok(b) => {
                    if b.is_empty() {
                        break;
                    }
                }
                Err(e) => return Err(Status::internal(e)),
            };
            let rb = RecordBatch::new_empty(SchemaRef::new(schema.schema_arrow.clone()));
            results.push(rb);
        }
        let flight_data = match batches_to_flight_data(&schema.schema_arrow, results) {
            Ok(data) => data,
            Err(e) => return Err(Status::internal(e.to_string())),
        }
        .into_iter()
        .map(Ok);
        Ok(Response::new(Box::pin(stream::iter(flight_data))))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        warn!("do_put not implemented");
        Err(Status::unimplemented("Implement do_put"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        warn!("do_exchange not implemented");
        Err(Status::unimplemented("Implement do_exchange"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        warn!("do_action not implemented");
        Err(Status::unimplemented("Implement do_action"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions: Vec<_> = vec![Ok(ActionType {
            r#type: "type 1".into(),
            description: "something".into(),
        })];
        let actions_stream = futures::stream::iter(actions);
        Ok(Response::new(actions_stream.boxed()))
    }
}
