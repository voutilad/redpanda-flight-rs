use apache_avro::types::Value;
use apache_avro::Reader;
use arrow::record_batch::RecordBatch;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use tracing::{error, warn};

use crate::schema::Schema;

/// Convert data to Arrow RecordBatches.
pub fn convert(messages: &Vec<OwnedMessage>, schema: &Schema) -> Result<RecordBatch, String> {
    let fields = schema.schema_arrow.fields.to_vec();

    // TODO: this probably needs optimization
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
        let record = match values.first().unwrap() {
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
