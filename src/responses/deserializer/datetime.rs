use crate::errors::SnowflakeError;
use crate::responses::types::{
    row_type::RowType,
    value::{Value, ValueType},
};

use anyhow::anyhow;
use chrono::{prelude::*, Duration};
use serde_json;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let pair = json.to_string();
    let timezone_str;
    let offset_str;
    match pair.split_once(" ") {
        Some(p) => (timezone_str, offset_str) = p,
        None => {
            return Err(SnowflakeError::new_deserialization_error_with_field_and_value(
                anyhow!("Expected timezone and offset pair, got {}", json),
                row_type.name.clone(),
                json.to_string(),
            ))
        }
    }

    let timestamp: f64 = serde_json::from_str(timezone_str).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;
    let offset: i32 = serde_json::from_str(offset_str).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    let timezone_opt = FixedOffset::east_opt((offset - 1440) * 60);
    let timezone;
    match timezone_opt {
        Some(tz) => timezone = tz,
        None => {
            return Err(SnowflakeError::new_deserialization_error_with_field_and_value(
                anyhow!("Invalid timezone offset {offset}"),
                row_type.name.clone(),
                json.to_string(),
            ))
        }
    }

    let nanos = (timestamp * 1_000_000_000.0).round() as i64;
    let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
    let datetime = DateTime::<FixedOffset>::from_local(naive, timezone);

    if row_type.nullable {
        let boxed = Box::new(Value::DateTime(datetime));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::DateTime(datetime))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &Box<dyn arrow2::array::Array>,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::PrimitiveArray;

    let mut err = Ok(());
    let downcasted = column.as_any().downcast_ref::<PrimitiveArray<i128>>().unwrap();
    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value;
            match x {
                Some(x) => value = i128::from(*x),
                None => return null_from_arrow(column, field),
            }

            if field.is_nullable {
                let boxed = Box::new(Value::Integer(value));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::Integer(value))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
