use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use chrono::{prelude::*, Duration};
use serde_json;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: f64 = serde_json::from_value(json.clone()).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    nanos_to_datetime((parsed * 1_000_000_000.0).round() as i64, row_type.nullable)
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::epoch::{arrow_int64_to_epoch, arrow_struct_to_epoch};
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use anyhow::anyhow;

    match &field.data_type {
        arrow2::datatypes::DataType::Int64 => arrow_int64_to_epoch(column, field)?
            .iter()
            .map(|e| match e {
                Some(e) => nanos_to_datetime(*e, field.is_nullable),
                None => null_from_arrow(field),
            })
            .collect(),
        arrow2::datatypes::DataType::Struct(..) => arrow_struct_to_epoch(column, field)?
            .iter()
            .map(|e| match e {
                Some(e) => nanos_to_datetime(*e, field.is_nullable),
                None => null_from_arrow(field),
            })
            .collect(),
        _ => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Invalid datetime data type {:?}", field.data_type),
            field.name.clone(),
        )),
    }
}

fn nanos_to_datetime(nanos: i64, is_nullable: bool) -> Result<Value, SnowflakeError> {
    let value = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);

    if is_nullable {
        let boxed = Box::new(Value::NaiveDateTime(value));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::NaiveDateTime(value))
    }
}
