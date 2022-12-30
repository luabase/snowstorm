use crate::errors::SnowflakeError;
use crate::responses::deserializer::epoch::duration_from_json_timestamp;
use crate::responses::types::{row_type::RowType, value::Value};

use anyhow::anyhow;
use chrono::prelude::*;
use serde_json;

pub(super) fn from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let pair = json.to_string();
    let timezone_str;
    let offset_str;
    match pair.split_once(' ') {
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

    let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + duration_from_json_timestamp(&timestamp);
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
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::epoch::get_arrow_time_scale;
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use arrow2::array::StructArray;
    use arrow2::scalar::PrimitiveScalar;
    use chrono::Duration;

    let _downcasted = column.as_any().downcast_ref::<StructArray>().unwrap();
    _downcasted
        .iter()
        .map(|e| match e {
            Some(value) => {
                let scalar = match value[0].as_any().downcast_ref::<PrimitiveScalar<i64>>() {
                    Some(d) => d.value(),
                    None => {
                        return Err(SnowflakeError::new_deserialization_error_with_field(
                            anyhow!("Could not deserialize epoch {:?} as i64", field.data_type),
                            field.name.clone(),
                        ))
                    }
                };

                let offset = match value[2].as_any().downcast_ref::<PrimitiveScalar<i32>>() {
                    Some(d) => match d.value() {
                        Some(dd) => dd,
                        None => {
                            return Err(SnowflakeError::new_deserialization_error_with_field(
                                anyhow!("Got null timezone offset"),
                                field.name.clone(),
                            ))
                        }
                    },
                    None => {
                        return Err(SnowflakeError::new_deserialization_error_with_field(
                            anyhow!("Could not deserialize timezone offset {:?} as i64", field.data_type),
                            field.name.clone(),
                        ))
                    }
                };

                match scalar {
                    Some(timestamp) => {
                        let timezone_opt = FixedOffset::east_opt((*offset - 1440) * 60);
                        let timezone;
                        match timezone_opt {
                            Some(tz) => timezone = tz,
                            None => {
                                return Err(SnowflakeError::new_deserialization_error_with_field_and_value(
                                    anyhow!("Invalid timezone offset {offset}"),
                                    field.name.clone(),
                                    timestamp.to_string(),
                                ))
                            }
                        }

                        let scale = get_arrow_time_scale(field)?;
                        let nanos = *timestamp * scale;
                        let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                        let datetime = DateTime::<FixedOffset>::from_local(naive, timezone);

                        if field.is_nullable {
                            let boxed = Box::new(Value::DateTime(datetime));
                            Ok(Value::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(Value::DateTime(datetime))
                        }
                    }
                    None => Err(SnowflakeError::new_deserialization_error_with_field(
                        anyhow!("Encountered null epoch value"),
                        field.name.clone(),
                    )),
                }
            }
            None => null_from_arrow(field),
        })
        .collect()
}
