use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use chrono::{prelude::*, Duration};
use serde_json;

pub(super) fn from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: f64 = serde_json::from_str(json).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    let nanos = (parsed * 10_f64.powf(9.0)).round() as i64;
    let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(nanos);
    if row_type.nullable {
        let boxed = Box::new(Value::NaiveTime(time));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::NaiveTime(time))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use anyhow::anyhow;
    use arrow2::array::PrimitiveArray;
    use arrow2::datatypes::DataType;

    match column.data_type() {
        DataType::Int32 => {
            let downcasted = match column.as_any().downcast_ref::<PrimitiveArray<i32>>() {
                Some(x) => x,
                None => {
                    return Err(SnowflakeError::new_deserialization_error_with_field(
                        anyhow!("Could not downcast to primitive array of i32"),
                        field.name.clone(),
                    ))
                }
            };

            downcasted.iter().map(|e| _arrow_timestamp_to_time(e, field)).collect()
        }
        DataType::Int64 => {
            let downcasted = match column.as_any().downcast_ref::<PrimitiveArray<i64>>() {
                Some(x) => x,
                None => {
                    return Err(SnowflakeError::new_deserialization_error_with_field(
                        anyhow!("Could not downcast to primitive array of i64"),
                        field.name.clone(),
                    ))
                }
            };

            downcasted.iter().map(|e| _arrow_timestamp_to_time(e, field)).collect()
        }
        _ => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Invalid data type {:?} for field {}", field.data_type, field.name),
            field.name.clone(),
        )),
    }
}

#[cfg(feature = "arrow")]
fn _arrow_timestamp_to_time<T: num::NumCast + Copy>(
    timestamp: Option<&T>,
    field: &arrow2::datatypes::Field,
) -> Result<Value, SnowflakeError> {
    use crate::responses::deserializer::epoch::{duration_from_timestamp_and_scale, get_arrow_time_scale};
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;

    match timestamp {
        Some(ts) => {
            let scale = get_arrow_time_scale(field)?;
            let cast: i64 = num::cast(*ts).unwrap();
            let value = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + duration_from_timestamp_and_scale(&cast, &scale);

            if field.is_nullable {
                let boxed = Box::new(Value::NaiveTime(value));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::NaiveTime(value))
            }
        }
        None => null_from_arrow(field),
    }
}
