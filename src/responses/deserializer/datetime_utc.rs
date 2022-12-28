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
    let nanos = (parsed * 1_000_000_000.0).round() as i64;
    let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
    let datetime = DateTime::<Utc>::from_utc(naive, Utc);

    if row_type.nullable {
        let boxed = Box::new(Value::DateTimeUTC(datetime));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::DateTimeUTC(datetime))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use anyhow::anyhow;
    use arrow2::array::StructArray;
    use arrow2::scalar::PrimitiveScalar;

    let _downcasted = column.as_any().downcast_ref::<StructArray>().unwrap();
    _downcasted
        .iter()
        .map(|e| match e {
            Some(value) => {
                let downcasted = (value[0]).as_any().downcast_ref::<PrimitiveScalar<i64>>();
                let scalar = match downcasted {
                    Some(d) => d.value(),
                    None => {
                        return Err(SnowflakeError::new_deserialization_error_with_field(
                            anyhow!("Could not deserialize {:?} as i64", field.data_type),
                            field.name.clone(),
                        ))
                    }
                };

                match scalar {
                    Some(x) => {
                        let nanos = *x * 1_000_000_000;
                        let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                        let datetime = DateTime::<Utc>::from_utc(naive, Utc);

                        if field.is_nullable {
                            let boxed = Box::new(Value::DateTimeUTC(datetime));
                            Ok(Value::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(Value::DateTimeUTC(datetime))
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
