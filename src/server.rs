use tracing::warn;

use futures::stream::{BoxStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::redpanda::Redpanda;
use crate::registry::Registry;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};

pub struct RedpandaFlightService {
    pub redpanda: Redpanda,
    pub registry: Registry,
    pub seeds: String,
}

impl RedpandaFlightService {
    pub fn new(seeds: &str, topic: &str) -> RedpandaFlightService {
        RedpandaFlightService {
            redpanda: Redpanda::connect(seeds).unwrap(),
            registry: Registry::new(topic, seeds).unwrap(),
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
        let vec: Vec<Result<FlightInfo, Status>> = Vec::new();
        let stream = futures::stream::iter(vec);
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
