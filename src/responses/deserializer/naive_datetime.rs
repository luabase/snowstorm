use crate::errors::SnowflakeError;
use crate::responses::deserializer::epoch::duration_from_json_timestamp;
use crate::responses::types::{row_type::RowType, value::Value};

use chrono::prelude::*;
use serde_json;

pub(super) fn from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let timestamp: f64 = serde_json::from_str(json).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    let datetime = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + duration_from_json_timestamp(&timestamp);

    let res = Value::NaiveDateTime(datetime);
    if row_type.nullable {
        let boxed = Box::new(res);
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(res)
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::epoch::{
        arrow_struct_to_naive_datetime, duration_from_arrow_timestamp_and_scale, get_arrow_time_scale,
    };
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use anyhow::anyhow;
    use arrow2::array::{PrimitiveArray, StructArray};

    match &field.data_type {
        arrow2::datatypes::DataType::Int64 => {
            let downcasted = match column.as_any().downcast_ref::<PrimitiveArray<i64>>() {
                Some(x) => x,
                None => {
                    return Err(SnowflakeError::new_deserialization_error_with_field(
                        anyhow!("Could not downcast to primitive array of i64"),
                        field.name.clone(),
                    ))
                }
            };

            let mut err = Ok(());
            let scale = get_arrow_time_scale(field)?;

            let res: Vec<Value> = downcasted
                .iter()
                .map(|e| match e {
                    Some(timestamp) => {
                        let datetime = NaiveDateTime::from_timestamp_opt(0, 0).unwrap()
                            + duration_from_arrow_timestamp_and_scale(timestamp, &scale);
                        Ok(Value::NaiveDateTime(datetime))
                    }
                    None => null_from_arrow(field),
                })
                .scan(&mut err, until_err)
                .collect();

            match err {
                Ok(..) => Ok(res),
                Err(e) => Err(e),
            }
        }

        arrow2::datatypes::DataType::Struct(..) => {
            let downcasted = match column.as_any().downcast_ref::<StructArray>() {
                Some(x) => x,
                None => {
                    return Err(SnowflakeError::new_deserialization_error_with_field(
                        anyhow!("Could not downcast to struct array"),
                        field.name.clone(),
                    ))
                }
            };

            let mut err = Ok(());

            let res = downcasted
                .values_iter()
                .map(|s| match arrow_struct_to_naive_datetime(&s, field)? {
                    Some(datetime) => {
                        let res = Value::NaiveDateTime(datetime);
                        if field.is_nullable {
                            let boxed = Box::new(res);
                            Ok(Value::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(res)
                        }
                    }
                    None => null_from_arrow(field),
                })
                .scan(&mut err, until_err)
                .collect();

            match err {
                Ok(..) => Ok(res),
                Err(e) => Err(e),
            }
        }

        _ => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Invalid data type {:?} for field {}", field.data_type, field.name),
            field.name.clone(),
        )),
    }
}
