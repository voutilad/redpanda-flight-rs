use std::string::String;

use apache_avro::schema as avro_schema;
use arrow::datatypes as arrow_datatypes;
use arrow::datatypes::{TimeUnit, UnionMode};

pub struct Schema {
    pub topic: String,
    pub id: i64,
    pub version: i64,
    schema_avro: avro_schema::RecordSchema,
    schema_arrow: arrow_datatypes::Schema,
}

fn avro_to_arrow_types(schema: &avro_schema::Schema) -> Result<arrow_datatypes::DataType, String> {
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
    pub fn from(json: String) -> Result<Schema, String> {
        let avro = avro_schema::Schema::parse_str(json.as_str()).unwrap();
        let record = match avro {
            avro_schema::Schema::Record(r) => r,
            _ => panic!("not a record schema!"),
        };
        let arrow = avro_to_arrow(&record).unwrap();
        Ok(Schema {
            topic: String::from(""),
            id: -1,
            version: -1,
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
    fn initializing() {
        let schema = Schema::from(String::from(SAMPLE_SCHEMA));
        assert!(schema.is_ok());
        let avro = schema?.schema_avro;
        let arrow = schema?.schema_arrow;
        assert_eq!(3, avro.fields.len(), "should have 3 Avro fields");
        assert_eq!(3, arrow.fields.len(), "should have 3 Arrow fields");
        assert_eq!(
            arrow_datatypes::DataType::Utf8,
            arrow.field_with_name("uuid").unwrap().data_type(),
            "uuid should be a utf8 string"
        );
        assert_eq!(
            arrow_datatypes::DataType::Timestamp(TimeUnit::Millisecond, None),
            arrow.field_with_name("timestamp"),
            "timestamp should be in millis"
        );
    }
}
