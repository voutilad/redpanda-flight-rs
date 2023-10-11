use std::string::String;

use apache_avro::schema as avro_schema;
use arrow::datatypes as arrow_datatypes;
use arrow::datatypes::{TimeUnit, UnionMode};
use serde::Deserialize;

/// Represents a known Redpanda Topic Schema.
pub struct Schema {
    pub topic: String,
    pub id: i64,
    pub version: i64,
    schema_avro: avro_schema::RecordSchema,
    schema_arrow: arrow_datatypes::Schema,
}

#[derive(Deserialize, Debug)]
pub struct RedpandaSchema {
    pub subject: String,
    pub version: u64,
    pub id: u64,
    pub schema: String,
}

fn avro_to_arrow_types(schema: &avro_schema::Schema) -> Result<arrow_datatypes::DataType, String> {
    /// Type mapping from an Avro RecordSchema field type to an Arrow DataType.
    /// TODO: Things get a bit messy with complex types, so skip most of those for now.
    match schema {
        avro_schema::Schema::Null => Ok(arrow_datatypes::DataType::Null),
        avro_schema::Schema::Boolean => Ok(arrow_datatypes::DataType::Boolean),
        avro_schema::Schema::Int => Ok(arrow_datatypes::DataType::Int32),
        avro_schema::Schema::Long => Ok(arrow_datatypes::DataType::Int64),
        avro_schema::Schema::Float => Ok(arrow_datatypes::DataType::Float32),
        avro_schema::Schema::Double => Ok(arrow_datatypes::DataType::Float64),
        avro_schema::Schema::Bytes => Ok(arrow_datatypes::DataType::Binary),
        avro_schema::Schema::String => Ok(arrow_datatypes::DataType::Utf8),
        avro_schema::Schema::Uuid => Ok(arrow_datatypes::DataType::Utf8),
        avro_schema::Schema::Date => Ok(arrow_datatypes::DataType::Date64),
        avro_schema::Schema::TimestampMillis => Ok(arrow_datatypes::DataType::Timestamp(
            TimeUnit::Millisecond,
            None,
        )),
        avro_schema::Schema::Union(u) => {
            // XXX nesting probably breaks this?
            let variants: Vec<arrow_datatypes::DataType> = u
                .variants()
                .iter()
                .map(avro_to_arrow_types)
                .map(|r| r.unwrap())
                .collect();
            let type_ids: Vec<i8> = (0..variants.len()).into_iter().map(|i| i as i8).collect();
            let fields: Vec<arrow_datatypes::Field> = variants
                .iter()
                .map(|v| arrow_datatypes::Field::new(v.to_string(), v.clone(), u.is_nullable()))
                .collect();
            Ok(arrow_datatypes::DataType::Union(
                arrow_datatypes::UnionFields::new(type_ids, fields),
                UnionMode::Dense,
            ))
        }
        _ => Err(String::from("unsupported Avro schema type")),
    }
}

fn avro_to_arrow(avro: &avro_schema::RecordSchema) -> Result<arrow_datatypes::Schema, String> {
    /// Convert a given Avro RecordSchema to an approximate Arrow schema.
    let fields: Vec<arrow_datatypes::Field> = avro
        .fields
        .iter()
        .map(|f| {
            println!("checking field {:?}", f.schema);
            let datatype = avro_to_arrow_types(&f.schema).unwrap();
            arrow_datatypes::Field::new(&f.name, datatype, f.is_nullable())
        })
        .collect();

    Ok(arrow_datatypes::Schema::new(fields))
}

impl Schema {
    pub fn from(value: &RedpandaSchema) -> Result<Schema, String> {
        let avro = avro_schema::Schema::parse_str(value.schema.as_str()).unwrap();
        let record = match avro {
            avro_schema::Schema::Record(r) => r,
            _ => return Err(String::from("not a record schema")),
        };
        let arrow = avro_to_arrow(&record).unwrap();
        let topic = String::from(value.subject.trim_end_matches("-value"));
        Ok(Schema {
            topic,
            id: value.id as i64,
            version: value.version as i64,
            schema_avro: record,
            schema_arrow: arrow,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static SAMPLE_SCHEMA: &str = include_str!("fixtures/sample_value_schema.json");

    #[test]
    fn can_convert_avro_to_arrow() {
        let input = RedpandaSchema {
            subject: String::from("sensor-value"),
            version: 1,
            id: 2,
            schema: String::from(SAMPLE_SCHEMA),
        };
        let schema = Schema::from(&input).unwrap();
        let avro = schema.schema_avro;
        let arrow = schema.schema_arrow;
        assert_eq!(3, avro.fields.len(), "should have 3 Avro fields");
        assert_eq!(3, arrow.fields.len(), "should have 3 Arrow fields");
        assert_eq!(
            arrow_datatypes::DataType::Utf8,
            arrow
                .field_with_name("identifier")
                .unwrap()
                .data_type()
                .clone(),
            "uuid should be a utf8 string"
        );
        assert_eq!(
            arrow_datatypes::DataType::Timestamp(TimeUnit::Millisecond, None),
            arrow
                .field_with_name("timestamp")
                .unwrap()
                .data_type()
                .clone(),
            "timestamp should be in millis"
        );
    }
}
