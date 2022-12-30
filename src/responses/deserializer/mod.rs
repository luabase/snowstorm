pub mod binary;
pub mod boolean;
pub mod datetime;
pub mod datetime_utc;
pub(self) mod epoch;
pub mod float;
pub mod hashmap;
pub mod integer;
pub mod naive_date;
pub mod naive_datetime;
pub mod naive_time;
pub mod null;
pub mod string;
pub mod variant;
pub mod vec;

use crate::errors::SnowflakeError;
use crate::responses::types::{
    row_type::RowType,
    value::{Value, ValueType},
};

#[cfg(feature = "arrow")]
use arrow2;
#[cfg(feature = "arrow")]
use arrow2::array::Array as ArrowArray;
#[cfg(feature = "arrow")]
use arrow2::chunk::Chunk as ArrowChunk;
#[cfg(feature = "arrow")]
use arrow2::datatypes::Field as ArrowField;
#[cfg(feature = "arrow")]
use arrow2::io::ipc::read::StreamMetadata as ArrowStreamMetadata;

#[cfg(feature = "arrow")]
type ArrowMetadataWithChunks = (ArrowStreamMetadata, Option<ArrowChunk<Box<dyn ArrowArray>>>);

pub trait QueryDeserializer: Sized {
    type ReturnType;

    fn deserialize_rowset(
        rowset: &[Vec<serde_json::Value>],
        rowtype: &[RowType],
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError>;

    #[cfg(feature = "arrow")]
    fn deserialize_rowset64(rowset: &str) -> Result<Vec<Self::ReturnType>, SnowflakeError>;

    #[cfg(not(feature = "arrow"))]
    fn deserialize_rowset64(_rowset: &str) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        panic!("Arrow feature is not enabled");
    }

    fn deserialize_value(value: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
        use crate::responses::deserializer::binary::from_json as binary_from_json;
        use crate::responses::deserializer::boolean::from_json as boolean_from_json;
        use crate::responses::deserializer::datetime::from_json as datetime_from_json;
        use crate::responses::deserializer::datetime_utc::from_json as datetime_utc_from_json;
        use crate::responses::deserializer::float::from_json as float_from_json;
        use crate::responses::deserializer::hashmap::from_json as hashmap_from_json;
        use crate::responses::deserializer::integer::from_json as integer_from_json;
        use crate::responses::deserializer::naive_date::from_json as naive_date_from_json;
        use crate::responses::deserializer::naive_datetime::from_json as naive_datetime_from_json;
        use crate::responses::deserializer::naive_time::from_json as naive_time_from_json;
        use crate::responses::deserializer::null::from_json as null_from_json;
        use crate::responses::deserializer::string::from_json as string_from_json;
        use crate::responses::deserializer::variant::from_json as variant_from_json;
        use crate::responses::deserializer::vec::from_json as vec_from_json;

        let json = match value.as_str() {
            Some(string) => string,
            None => return null_from_json(row_type),
        };

        let value_type = match row_type.value_type() {
            ValueType::Nullable(v) => *v,
            _ => row_type.value_type(),
        };

        match value_type {
            ValueType::Boolean => boolean_from_json(json, row_type),
            ValueType::Integer => integer_from_json(json, row_type),
            ValueType::Float => float_from_json(json, row_type),
            ValueType::String => string_from_json(json, row_type),
            ValueType::Binary => binary_from_json(json, row_type),
            ValueType::NaiveDate => naive_date_from_json(json, row_type),
            ValueType::NaiveTime => naive_time_from_json(json, row_type),
            ValueType::NaiveDateTime => naive_datetime_from_json(json, row_type),
            ValueType::DateTimeUTC => datetime_utc_from_json(json, row_type),
            ValueType::DateTime => datetime_from_json(json, row_type),
            ValueType::Variant => variant_from_json(value, row_type),
            ValueType::HashMap => hashmap_from_json(json, row_type),
            ValueType::Vec => vec_from_json(json, row_type),
            _ => {
                if row_type.nullable {
                    let boxed = Box::new(Value::Unsupported(value.to_owned()));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Unsupported(value.to_owned()))
                }
            }
        }
    }

    #[cfg(feature = "arrow")]
    fn get_arrow_stream_from_rowset64(rowset: &str) -> Result<Option<ArrowMetadataWithChunks>, SnowflakeError> {
        use arrow2::io::ipc::read;

        if rowset.is_empty() {
            return Ok(None);
        }

        let data = base64::decode(rowset).map_err(|e| SnowflakeError::new_deserialization_error(e.into()))?;
        let mut stream: &[u8] = &data;

        let metadata =
            read::read_stream_metadata(&mut stream).map_err(|e| SnowflakeError::new_deserialization_error(e.into()))?;

        let mut stream = read::StreamReader::new(&mut stream, metadata.clone(), None);

        if let Some(x) = stream.next() {
            match x {
                Ok(read::StreamState::Some(chunk)) => Ok(Some((metadata, Some(chunk)))),
                Ok(read::StreamState::Waiting) => Ok(Some((metadata, None))),
                Err(e) => Err(SnowflakeError::new_deserialization_error(e.into())),
            }
        }
        else {
            Ok(Some((metadata, None)))
        }
    }

    #[cfg(feature = "arrow")]
    fn deserialize_arrow_column(column: &dyn ArrowArray, field: &ArrowField) -> Result<Vec<Value>, SnowflakeError> {
        use crate::responses::deserializer::binary::from_arrow as binary_from_arrow;
        use crate::responses::deserializer::boolean::from_arrow as boolean_from_arrow;
        use crate::responses::deserializer::datetime::from_arrow as datetime_from_arrow;
        use crate::responses::deserializer::datetime_utc::from_arrow as datetime_utc_from_arrow;
        use crate::responses::deserializer::float::from_arrow as float_from_arrow;
        use crate::responses::deserializer::hashmap::from_arrow as hashmap_from_arrow;
        use crate::responses::deserializer::integer::from_arrow as integer_from_arrow;
        use crate::responses::deserializer::naive_date::from_arrow as naive_date_from_arrow;
        use crate::responses::deserializer::naive_datetime::from_arrow as naive_datetime_from_arrow;
        use crate::responses::deserializer::naive_time::from_arrow as naive_time_from_arrow;
        use crate::responses::deserializer::string::from_arrow as string_from_arrow;
        use crate::responses::deserializer::variant::from_arrow as variant_from_arrow;
        use crate::responses::deserializer::vec::from_arrow as vec_from_arrow;
        use anyhow::anyhow;

        let row_type = RowType::from_arrow_field(field);

        let value_type = match row_type.value_type() {
            ValueType::Nullable(v) => *v,
            _ => row_type.value_type(),
        };

        match value_type {
            ValueType::Boolean => boolean_from_arrow(column, field),
            ValueType::Integer => integer_from_arrow(column, field),
            ValueType::Float => match &field.data_type {
                arrow2::datatypes::DataType::Int8 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::UInt8 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::Int16 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::UInt16 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::Int32 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::UInt32 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::Int64 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::UInt64 => integer_from_arrow(column, field),
                arrow2::datatypes::DataType::Float16 => float_from_arrow(column, field),
                arrow2::datatypes::DataType::Float32 => float_from_arrow(column, field),
                arrow2::datatypes::DataType::Float64 => float_from_arrow(column, field),
                x => Err(SnowflakeError::new_deserialization_error_with_field(
                    anyhow!("Invalid float data type {:?}", x),
                    field.name.clone(),
                )),
            },
            ValueType::String => string_from_arrow(column, field),
            ValueType::Binary => binary_from_arrow(column, field),
            ValueType::NaiveDate => naive_date_from_arrow(column, field),
            ValueType::NaiveTime => naive_time_from_arrow(column, field),
            ValueType::NaiveDateTime => naive_datetime_from_arrow(column, field),
            ValueType::DateTimeUTC => datetime_utc_from_arrow(column, field),
            ValueType::DateTime => datetime_from_arrow(column, field),
            ValueType::Variant => variant_from_arrow(column, field),
            ValueType::HashMap => hashmap_from_arrow(column, field),
            ValueType::Vec => vec_from_arrow(column, field),
            x => Err(SnowflakeError::new_deserialization_error_with_field(
                anyhow!("Unrecognized value data type {:?}", x),
                field.name.clone(),
            )),
        }
    }
}
