use std::any::Any;
use std::collections::HashMap;
use std::ops::Deref;

use apache_avro::types::Value;
use apache_avro::Reader;
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date64Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, NullBuilder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, ToByteSlice};
use arrow::record_batch::RecordBatch;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use tracing::{error, warn};

use crate::schema::Schema;

/// Convert data to Arrow RecordBatches.
/// N.b. this is CPU bound. Might need offloading via [tokio::spawn_blocking].
/// XXX there be dragons
pub fn convert(messages: &Vec<OwnedMessage>, schema: &Schema) -> Result<RecordBatch, String> {
    let avro_schema = apache_avro::Schema::Record(schema.avro.clone());
    let fields = schema.arrow.fields.to_vec();

    // Do some brute forcing here to sort our builders in Avro field order. This will allow
    // indexing into the vectors during value conversion. This should probably be cached, but
    // not sure if we can guarantee the order each time.
    // N.b. we need an ordered list of builders, so we can't just use a map of field -> builder.
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut builders_idx: HashMap<String, usize> = HashMap::new();
    let mut builder_types: Vec<DataType> = Vec::new();

    for avro_field in schema.avro.fields.iter() {
        let f = fields.iter().find(|&f| f.name() == &avro_field.name);
        if f.is_none() {
            return Err(String::from(format!(
                "failed to find aligning fields for '{}'",
                avro_field.name
            )));
        }
        let builder: Box<dyn ArrayBuilder> = match f.unwrap().data_type() {
            DataType::Null => Box::new(NullBuilder::new()),
            DataType::Boolean => Box::new(BooleanBuilder::new()), // Avro Boolean
            DataType::Int32 => Box::new(Int32Builder::new()),     // Avro int
            DataType::Int64 => Box::new(Int64Builder::new()),     // Avro long
            DataType::Float32 => Box::new(Float32Builder::new()), // Avro float
            DataType::Float64 => Box::new(Float64Builder::new()), // Avro double
            DataType::Binary => Box::new(BinaryBuilder::new()),   // Avro binary
            DataType::Utf8 => Box::new(StringBuilder::new()),     // Avro String
            DataType::Date64 => Box::new(Date64Builder::new()),   // Avro Date
            DataType::Timestamp(_, _) => Box::new(TimestampMillisecondBuilder::new()), // Avro TimestampMillis
            _ => return Err(String::from("unsupported Arrow field type")),
        };
        builders.push(builder);
        builders_idx.insert(f.unwrap().name().clone(), builders.len() - 1);
        builder_types.push(f.unwrap().data_type().clone());
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
        // TODO: remove this debug stuff.
        if !payload.starts_with(&[b'O', b'b', b'j', 1u8]) {
            let junk = payload.to_byte_slice();
            warn!(
                "bad payload? first few bytes don't look right, got {:?}",
                junk
            );
            continue;
        }
        let reader = match Reader::with_schema(&avro_schema, payload) {
            Ok(r) => r,
            Err(e) => {
                error!("error reading payload: {}", e);
                // return Err(e.to_string());
                continue;
            }
        };

        // Decode the Record.
        // XXX TODO: we assume each payload is a single avro Record (i.e. a single row) for now.
        let mut values: Vec<Value> = Vec::new();
        for value in reader {
            match value {
                Ok(v) => values.push(v),
                Err(e) => {
                    error!("failed to read value from payload: {}", e);
                    return Err(String::from("failed to read value from payload"));
                }
            };
            break;
        }
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
        for idx in 0..record.len() {
            let (field, value) = record.get(idx).unwrap();
            let idx = builders_idx.get(field).unwrap();
            let builder = builders.get_mut(*idx).unwrap();
            match value {
                Value::Boolean(b) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap()
                        .append_value(*b);
                }
                Value::Int(i) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .unwrap()
                        .append_value(*i);
                }
                Value::Long(l) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .unwrap()
                        .append_value(*l);
                }
                Value::Float(f) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .unwrap()
                        .append_value(*f);
                }
                Value::Double(d) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap()
                        .append_value(*d);
                }
                Value::Bytes(b) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .unwrap()
                        .append_value(b);
                }
                Value::String(s) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(s.as_str());
                }
                Value::Uuid(u) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(u.as_hyphenated().to_string().as_str()); // TODO: is this correct?
                }
                Value::TimestampMillis(ts) => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                        .unwrap()
                        .append_value(*ts);
                }
                Value::Union(_, inner_value) => {
                    match inner_value.deref() {
                        Value::Null => {
                            // Fallback to schema to determine type.
                            // Need to fallback to type hint vector.
                            match builder_types.get(*idx).unwrap() {
                                DataType::Boolean => builder
                                    .as_any_mut()
                                    .downcast_mut::<BooleanBuilder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Int32 => builder
                                    .as_any_mut()
                                    .downcast_mut::<Int32Builder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Int64 => builder
                                    .as_any_mut()
                                    .downcast_mut::<Int64Builder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Float32 => builder
                                    .as_any_mut()
                                    .downcast_mut::<Float32Builder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Float64 => builder
                                    .as_any_mut()
                                    .downcast_mut::<Float64Builder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Timestamp(_, _) => builder
                                    .as_any_mut()
                                    .downcast_mut::<TimestampMillisecondBuilder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Date64 => builder
                                    .as_any_mut()
                                    .downcast_mut::<Date64Builder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Binary => builder
                                    .as_any_mut()
                                    .downcast_mut::<BinaryBuilder>()
                                    .unwrap()
                                    .append_null(),
                                DataType::Utf8 => builder
                                    .as_any_mut()
                                    .downcast_mut::<StringBuilder>()
                                    .unwrap()
                                    .append_null(),
                                _ => panic!("unimplemented type"),
                            }
                        }
                        Value::Boolean(b) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<BooleanBuilder>()
                                .unwrap()
                                .append_value(*b);
                        }
                        Value::Int(i) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<Int32Builder>()
                                .unwrap()
                                .append_value(*i);
                        }
                        Value::Long(l) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<Int64Builder>()
                                .unwrap()
                                .append_value(*l);
                        }
                        Value::Float(f) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<Float32Builder>()
                                .unwrap()
                                .append_value(*f);
                        }
                        Value::Double(d) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<Float64Builder>()
                                .unwrap()
                                .append_value(*d);
                        }
                        Value::Bytes(b) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<BinaryBuilder>()
                                .unwrap()
                                .append_value(b);
                        }
                        Value::String(s) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap()
                                .append_value(s.as_str());
                        }
                        Value::Uuid(u) => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap()
                                .append_value(u.as_hyphenated().to_string().as_str());
                            // TODO: is this correct?
                        }
                        _ => panic!("unimplemented inner value type: {:?}", value.type_id()),
                    }
                }
                _ => panic!("unimplemented value type: {:?}", value.type_id()),
            }
        } // Convert
    }

    // Finalize buffers and build the RecordBatch!
    let cols: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
    match RecordBatch::try_new(schema.arrow.clone(), cols) {
        Ok(r) => Ok(r),
        Err(e) => {
            error!("failed to build record batch: {}", e);
            return Err(e.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::types::Value::Union;
    use apache_avro::types::{Record, Value};
    use arrow::array::{Array, Int64Array};
    use rdkafka::message::OwnedMessage;
    use rdkafka::Timestamp;

    use crate::convert::convert;
    use crate::schema::{RedpandaSchema, Schema};

    static SAMPLE_SCHEMA: &str = include_str!("fixtures/sample_value_schema.json");

    #[test]
    fn can_convert_avro_record_to_arrow_record_batch() {
        let input = RedpandaSchema {
            subject: String::from("sensor-value"),
            version: 1,
            id: 2,
            schema: String::from(SAMPLE_SCHEMA),
        };
        let schema = Schema::from(&input).unwrap();
        let avro_schema = apache_avro::Schema::Record(schema.avro.clone());
        // encode some data
        let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
        let mut record1 = Record::new(&avro_schema).unwrap();
        record1.put("timestamp", Value::TimestampMillis(1697643448668i64));
        record1.put("identifier", Value::Uuid(uuid::Uuid::new_v4()));
        record1.put("value", Union(1, Box::new(Value::Long(1234))));
        writer.append(record1).unwrap();
        let payload1 = writer.into_inner().unwrap();

        writer = apache_avro::Writer::new(&avro_schema, Vec::new());
        let mut record2 = Record::new(&avro_schema).unwrap();
        record2.put("timestamp", Value::TimestampMillis(1697643448668i64));
        record2.put("identifier", Value::Uuid(uuid::Uuid::new_v4()));
        record2.put("value", Union(1, Box::new(Value::Long(5678))));
        writer.append(record2).unwrap();
        let payload2 = writer.into_inner().unwrap();

        writer = apache_avro::Writer::new(&avro_schema, Vec::new());
        let mut record3 = Record::new(&avro_schema).unwrap();
        record3.put("timestamp", Value::TimestampMillis(1697643448668i64));
        record3.put("identifier", Value::Uuid(uuid::Uuid::new_v4()));
        record3.put("value", Union(0, Box::new(Value::Null)));
        writer.append(record3).unwrap();
        let payload3 = writer.into_inner().unwrap();

        let messages = vec![
            OwnedMessage::new(
                Some(payload1),
                Some(String::from("key1").into_bytes()),
                String::from("topic"),
                Timestamp::CreateTime(0),
                1,
                0,
                None,
            ),
            OwnedMessage::new(
                Some(payload2),
                Some(String::from("key2").into_bytes()),
                String::from("topic"),
                Timestamp::CreateTime(1),
                1,
                1,
                None,
            ),
            OwnedMessage::new(
                Some(payload3),
                Some(String::from("key3").into_bytes()),
                String::from("topic"),
                Timestamp::CreateTime(1),
                1,
                1,
                None,
            ),
        ];

        let batch = convert(&messages, &schema).unwrap();
        assert_eq!(
            3,
            batch.columns().len(),
            "should have 3 columns based on the schema json"
        );
        assert_eq!(3, batch.num_rows(), "should have 3 rows");
        assert_eq!(
            vec![1234, 5678, 0], // XXX unclear what the docs mean by null values being "arbitrary"
            batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            "the 'value' field should contain the intended vector of numbers"
        );
    }
}
