use apache_avro::types::Value;
use apache_avro::Reader;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date64Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use tracing::{error, warn};

use crate::schema::Schema;

/// Convert data to Arrow RecordBatches.
/// N.b. this is CPU bound. Might need offloading via [tokio::spawn_blocking].
pub fn _convert(messages: &Vec<OwnedMessage>, schema: &Schema) -> Result<RecordBatch, String> {
    let fields = schema.schema_arrow.fields.to_vec();

    // Do some brute forcing here to sort our builders in Avro field order. This will allow
    // indexing into the vectors during value conversion. This should probably be cached, but
    // not sure if we can guarantee the order each time.
    //
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    for avro_field in &schema.record_schema.fields {
        let f = fields.iter().find(|&f| f.name() == &avro_field.name);
        if f.is_none() {
            return Err(String::from(format!(
                "failed to find aligning fields for '{}'",
                avro_field.name
            )));
        }
        let builder: Box<dyn ArrayBuilder> = match f.unwrap().data_type() {
            DataType::Boolean => Box::new(BooleanBuilder::new()), // Avro Boolean
            DataType::Int32 => Box::new(Int32Builder::new()),     // Avro int
            DataType::Int64 => Box::new(Int64Builder::new()),     // Avro long
            DataType::Float32 => Box::new(Float32Builder::new()), // Avro float
            DataType::Float64 => Box::new(Float64Builder::new()), // Avro double
            DataType::Binary => Box::new(BinaryBuilder::new()),   // Avro binary
            DataType::Utf8 => Box::new(StringBuilder::new()),     // Avro String
            // DataType::Union(_, _) => {},                 // TODO
            DataType::Date64 => Box::new(Date64Builder::new()), // Avro Date
            DataType::Timestamp(_, _) => Box::new(TimestampMillisecondBuilder::new()), // Avro TimestampMillis
            _ => return Err(String::from("unsupported Arrow field type")),
        };
        builders.push(builder);
    }

    // Process each message.
    for message in messages {
        // Build our Apache Avro reader from our payload. We provide the expected schema.
        //
        let payload = match message.payload() {
            None => {
                warn!("empty record in payload");
                break;
            }
            Some(p) => p,
        };
        let reader = match Reader::with_schema(&schema.schema_avro, payload) {
            Ok(r) => r,
            Err(e) => {
                error!("error reading payload: {}", e);
                return Err(e.to_string());
            }
        };

        // Decode the Record.
        // XXX TODO: we assume each payload is a single avro record (i.e. a single row) for now.
        let values = match reader.collect::<Result<Vec<Value>, _>>() {
            Ok(v) => v,
            Err(_) => {
                error!("failed to read value from payload");
                return Err(String::from("failed to read value from payload"));
            }
        };
        if values.len() != 1 {
            error!("bogus or multi-record value from payload");
            return Err(String::from("expected single-value payload"));
        }
        let _record = match values.first().unwrap() {
            Value::Record(r) => r,
            _ => {
                error!("payload is not an avro record");
                return Err(String::from("payload is not an avro record"));
            }
        };

        // Convert!
        //
    }
    todo!();
}
