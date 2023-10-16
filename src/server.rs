use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::stream::{BoxStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, warn};

use crate::redpanda::{BatchingStream, Redpanda, Topic, TopicPartition};
use crate::registry::Registry;

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
                                    "/",
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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        warn!("do_get not implemented");

        let tp = TopicPartition {
            topic: "".to_string(),
            id: 0,
            watermarks: (0, 0),
            bytes: 0,
        };
        let stream = match self.redpanda.stream(&tp).await {
            Ok(s) => s,
            Err(e) => {
                error!("failed to create stream for {:?}", tp);
                return Err(Status::internal(e));
            }
        };
        stream.next_batch().await.unwrap();
        Err(Status::unimplemented("Implement do_get"))
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
