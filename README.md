# Redpanda Flight

> "There is an art, it says, or rather, a knack to flying. The knack lies in learning how to throw yourself at the
> ground and miss."
>
> — Douglas Adams, _The Hitchhiker's Guide to the Galaxy_

This is a prototype of an [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) proxy for Redpanda.
Put simply, it exposes a topic as a Flight stream, consumable via an Apache Arrow Flight client.

## Why?

A few reasons...

* I needed to learn about how the Rust [rdkafka](https://crates.io/crates/rdkafka) crate works and relearn a bit I've
  forgotten about Rust in general.
* I'm a big believer in the power of Flight, having built [abstractions](https://github.com/neo4j-field/neo4j-arrow) for
  other stacks in the past.
* I needed to learn more about how the Schema Registry works in the Kafka world.

## Who might ever use this?

Honestly, not sure, but if someone needs to quickly build a service that hydrates state from a topic _en masse_, Flight
gives a quick and efficient way to fetch all that data without dealing with all the Kafka API Consumer nonsense.

## Ok, I just want to try it.

Sure, why not. It does work with Redpanda Cloud if you're lazy.

### Building

Thankfully Rust's tooling is top-notch. Just make sure you've got Rust and Cargo (tested with 1.72 and newer).

```
$ cargo build --release
```

### Running

All config is pretty bare bones and via environment args at the moment:

* `REDPANDA_BROKERS` - broker seed address(es) (default: `localhost:9092`)
* `REDPANDA_SCHEMA_TOPIC` - topic for the schema registry (default: `_schemas`)
* `REDPANDA_SASL_USERNAME` - username for the proxy's connection to Redpanda (optional)
* `REDPANDA_SASL_PASSWORD` - password for the proxy's connection to Redpanda (optional)
* `REDPANDA_SASL_MECHANISM` - sasl mechanism for the proxy's connection to Redpanda (optional)
* `REDPANDA_TLS_ENABLED` - whether to use TLS for the connection to Redpanda (default: no)

You can change the log level by setting `RUST_LOG` to `debug` or `info`, etc.

### Caveats!

* do **NOT** use in production
    - no front-end TLS support yet, so client's talk unencrypted to the proxy
    - while the client's pass their Kafka login info at request time to the proxy, need to audit areas to minimize
      risk/exposure. At any given time the proxy process will have the user's password in memory somewhere.
    - basic auth is used only for `do_get` api calls (fetching data) and not all api calls yet
* expects a topic with an Avro schema in the Schema Registry
    - not all Avro types are supported: for now use a `record` with basic primitive fields
    - nullable types are approximated via Avro `union` fields, but assume a simple `["null", <primitive>]` value format.
    - datetime/timestamp stuff hasn't been tested
    - some logical types may be mapped to simpler Arrow types (e.g. uuid -> utf8)
* no performance tuning
    - still need to learn more about how to build an efficient async stream
    - batching has to occur, but not sure best way to do it
* generates lots of consumer groups
    - while not using "subscribe", the Rust rdkafka library I'm using doesn't expose a direct way to consume data
      without causing the creation of a consumer group

### Simple PyArrow Example

Make sure you've created a topic and that topic has an Avro schema registered properly.

An example schema:

```json
{
  "type": "record",
  "name": "flight_test",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int",
      "default": -1
    },
    {
      "name": "nullable",
      "type": [
        "null",
        "float"
      ]
    }
  ]
}
```

You can generate data using Python and the `confluent-kafka` library. _(Sample code coming.)_

For the consuming client side, assuming you `pip install`'d `pyarrow` ideally in a virtualenv:

```python
#!/usr/bin/env python3
import pyarrow.flight
import base64
import os

# we use basic auth for now
username = os.environ.get("REDPANDA_SASL_USERNAME")
password = os.environ.get("REDPANDA_SASL_PASSWORD")
auth_token = b"Basic " + base64.b64encode(f"{username}:{password}".encode())

# connecting to localhost without TLS(!!)
client = pyarrow.flight.FlightClient("grpc+tcp://127.0.0.1:9999")

# setup http headers for auth
opts = pyarrow.flight.FlightCallOptions(
        headers=[(b"authorization", auth_token)],
        timeout=10,
)

# list flights/topics avialable
print("Fetching flight infos:")
for info in client.list_flights(options=opts):
        print(info)
print("----------------------\n\n")

# stream data from a topic partition into a PyArrow Table
# note: this may take a few seconds as it streams and assembles batches
ticket = pyarrow.flight.Ticket(b"flight_test/0")
print(f"Fetching flight with ticket {ticket}")
table = client.do_get(ticket, options=opts).read_all()
print(table)
```

# Copyright & License

I put this together at `${DAYJOB}`, so it's Copyright 2023 Redpanda Data and provided under the Apache-2.0 License.