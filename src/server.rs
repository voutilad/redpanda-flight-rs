use std::str::FromStr;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use base64::Engine;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::redpanda::{Auth, AuthMechanism, AuthProtocol, Redpanda, Topic};
use crate::registry::Registry;

/// Used to join a topic name and a partition id to form a ticket. By choice, this separator
/// contains value(s) not valid for Apache Kafka Topics.
const TICKET_SEPARATOR: &str = "/";
const TICKET_SEPARATOR_BYTE: &u8 = &b'/';

pub struct RedpandaFlightService {
    pub redpanda: Redpanda,
    pub registry: Registry,
    pub seeds: String,

    require_auth: bool,
    auth_mechanism: AuthMechanism,
    auth_protocol: AuthProtocol,
}

impl RedpandaFlightService {
    pub async fn new(
        seeds: &str,
        schemas_topic: &str,
        admin_auth: Option<Auth>,
    ) -> Result<RedpandaFlightService, String> {
        let redpanda = match Redpanda::connect(seeds, &admin_auth).await {
            Ok(r) => r,
            Err(e) => {
                error!("failed to create Redpanda service: {}", e);
                return Err(e);
            }
        };
        let registry = match Registry::new(schemas_topic, seeds, &admin_auth).await {
            Ok(r) => r,
            Err(e) => {
                error!("failed to create Registry service: {}", e);
                return Err(e);
            }
        };

        // TODO: for now we assume the client auth mechanism and protocols match the backend.
        // N.b. This means the client can't use a different SASL SCRAM mechanism.
        if admin_auth.is_some() {
            let auth = admin_auth.unwrap();
            Ok(RedpandaFlightService {
                redpanda,
                registry,
                seeds: String::from(seeds),
                auth_mechanism: auth.mechanism.clone(),
                auth_protocol: auth.protocol.clone(),
                require_auth: true,
            })
        } else {
            Ok(RedpandaFlightService {
                redpanda,
                registry,
                seeds: String::from(seeds),
                auth_mechanism: AuthMechanism::Plain,
                auth_protocol: AuthProtocol::Plaintext,
                require_auth: false,
            })
        }
    }
}

/// Decode a Basic Auth HTTP header into an [`Auth`] assuming the provided mechanism and protocol.
/// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization
/// Returns [`None`] on failure.
fn parse_basic_auth(
    bytes: &[u8],
    mechanism: &AuthMechanism,
    protocol: &AuthProtocol,
) -> Option<Auth> {
    if bytes.len() < "Basic ".as_bytes().len() {
        debug!("malformed auth header: too short");
        return None;
    }

    // Payload should be small, so copying it shouldn't be an issue.
    let raw = match String::from_utf8(Vec::from(bytes.strip_prefix(b"Basic ")?)) {
        Ok(r) => r,
        Err(_) => {
            warn!("malformed auth header: not basic auth");
            return None;
        }
    };
    let decoded = match base64::engine::general_purpose::STANDARD.decode(raw) {
        Ok(d) => d,
        Err(e) => {
            warn!("malformed auth header: {}", e);
            return None;
        }
    };

    let idx = decoded.iter().position(|c| c == &b':').unwrap_or(0);
    if idx == 0 {
        debug!("malformed auth header: missing delimiter");
        return None;
    }
    let (username, mut password) = decoded.split_at(idx);

    // strip the : prefix
    password = password.strip_prefix(b":")?;
    password = password.strip_suffix(b"\n")?;

    Some(Auth {
        username: String::from_utf8_lossy(username).to_string(),
        password: String::from_utf8_lossy(password).to_string(),
        protocol: protocol.clone(),
        mechanism: mechanism.clone(),
    })
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

        let topics: Vec<Topic> = match self.redpanda.list_topics().await {
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
            let result = match FlightInfo::new().try_with_schema(&schema.arrow) {
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
        Err(Status::unimplemented("get_flight_info not implemented"))
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
        let info = FlightInfo::new().try_with_schema(&schema.arrow).unwrap();
        Ok(Response::new(SchemaResult {
            schema: info.schema,
        }))
    }
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // If we're using auth on the backend, we need to look for a basic auth header here. We
        // need to masquerade as the client/user, sadly. (That means we have a target on our backs.)
        let auth: Option<Auth> = request
            .metadata()
            .clone()
            .into_headers()
            .get("authorization")
            .map_or(None, |v| {
                parse_basic_auth(v.as_bytes(), &self.auth_mechanism, &self.auth_protocol)
            });

        if auth.is_none() && self.require_auth {
            debug!("missing auth! headers: {:?}", request.metadata());
            return Err(Status::unauthenticated(
                "unauthorized: no authentication token provided",
            ));
        }

        // Decompose ticket
        // N.b. Apache Kafka topics are ascii.
        // See Apache Kafka's clients/src/main/java/org/apache/kafka/common/internals/Topic.java
        let ticket = request.into_inner().ticket;
        if !ticket.is_ascii() {
            return Err(Status::failed_precondition(
                "redpanda-flight tickets must be ascii",
            ));
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

        //
        // The TICKET_SEPARATOR is not a valid topic name character, so there should be 1 occurrence.
        //
        let idx = ticket
            .iter()
            .rposition(|c| c == TICKET_SEPARATOR_BYTE)
            .unwrap_or(0);
        let parts = ticket.split_at(idx);
        debug!("parsing ticket bytes: {:?}", parts);

        let topic = match String::from_utf8(parts.0.to_vec()) {
            Err(e) => {
                warn!("problem parsing topic: {}", e);
                return Err(Status::failed_precondition(
                    "bad topic in redpanda-flight ticket",
                ));
            }
            Ok(t) => t,
        };
        if parts.1.len() < 2 {
            // First byte should be b'/', subsequent should be the digits
            warn!("problem parsing partition id: does not look to contain separator and digits");
            return Err(Status::failed_precondition(
                "invalid partition id in redpanda-flight ticket",
            ));
        }
        let pid = match String::from_utf8(parts.1.split_at(1).1.to_vec()) {
            Ok(s) => match i32::from_str(s.as_str()) {
                Ok(pid) => pid,
                Err(e) => {
                    warn!("problem parsing partition id: {}", e);
                    return Err(Status::failed_precondition(
                        "bad partition id in redpanda-flight ticket",
                    ));
                }
            },
            Err(e) => {
                warn!("bad partition id: {}", e);
                return Err(Status::failed_precondition(
                    "bad partition id in redpanda-flight ticket",
                ));
            }
        };

        //
        // Now that we know the intended topic, make sure we've got a schema.
        //
        let schema = match self.registry.lookup(topic.as_str()).await {
            None => return Err(Status::not_found("no matching schema found")),
            Some(s) => s,
        };

        // TODO: do we need a cache of watermarks? or just do another lookup via the kafka api?
        let tp = match self.redpanda.get_topic_partition(topic.as_str(), pid).await {
            Ok(tp) => tp,
            Err(e) => {
                error!(
                    "failed to find topic partition for {}/{}: {}",
                    topic, pid, e
                );
                return Err(Status::not_found("no matching topic partition found"));
            }
        };
        let batches = match self.redpanda.stream(&tp, &schema, auth).await {
            Ok(bs) => bs,
            Err(e) => {
                error!("failed to create stream for {:?}", tp);
                return Err(Status::internal(e));
            }
        };

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema.arrow)
            .with_flight_descriptor(None)
            .build(batches)
            .map_err(Into::into); // Needed to unpack the FlightError's into Status's

        info!("responding to do_get for topic partition {}/{}", topic, pid);
        Ok(Response::new(stream.boxed()))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put is not implemented"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not implemented"))
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

#[cfg(test)]
mod tests {
    use tracing::info;

    use crate::redpanda::{AuthMechanism, AuthProtocol};
    use crate::server::parse_basic_auth;

    const GOOD_SAMPLE1_IN: &str = "Basic bXktdXNlcm5hbWU6bXktc3VwZXItc2VjcmV0OnBhc3N3b3JkIV4K";
    const GOOD_SAMPLE1_USER: &str = "my-username";
    const GOOD_SAMPLE1_PASS: &str = r#"my-super-secret:password!^"#;

    const GOOD_SAMPLE2_IN: &str = "Basic bXktdXNlcm5hbWU6bXktc3VwZXItc2VjcmV0OnBhc3N3b3JkIV4yCg==";
    const GOOD_SAMPLE2_USER: &str = "my-username";
    const GOOD_SAMPLE2_PASS: &str = r#"my-super-secret:password!^2"#;

    #[test]
    fn can_convert_basic_auth() {
        let mechanism = AuthMechanism::ScramSha512;
        let protocol = AuthProtocol::SaslSsl;

        let mut maybe_auth = parse_basic_auth(GOOD_SAMPLE1_IN.as_bytes(), &mechanism, &protocol);
        assert!(maybe_auth.is_some(), "should have a result");

        let mut auth = maybe_auth.unwrap();
        info!("auth.password = {}", auth.password);
        assert_eq!(mechanism, auth.mechanism, "mechanism should be unchanged");
        assert_eq!(protocol, auth.protocol, "protocol should be unchanged");
        assert_eq!(GOOD_SAMPLE1_USER, auth.username, "username should match");
        assert_eq!(GOOD_SAMPLE1_PASS, auth.password, "password should match");

        maybe_auth = parse_basic_auth(GOOD_SAMPLE2_IN.as_bytes(), &mechanism, &protocol);
        assert!(maybe_auth.is_some(), "should have a result");
        auth = maybe_auth.unwrap();
        assert_eq!(mechanism, auth.mechanism, "mechanism should be unchanged");
        assert_eq!(protocol, auth.protocol, "protocol should be unchanged");
        assert_eq!(GOOD_SAMPLE2_USER, auth.username, "username should match");
        assert_eq!(GOOD_SAMPLE2_PASS, auth.password, "password should match");
    }
}
