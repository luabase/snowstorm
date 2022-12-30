#[cfg(feature = "arrow")]
use crate::errors::SnowflakeError;
#[cfg(feature = "arrow")]
use anyhow::anyhow;
#[cfg(feature = "arrow")]
use arrow2::{
    datatypes::{DataType, Field},
    scalar::Scalar,
};
#[cfg(feature = "arrow")]
use chrono::prelude::*;
use chrono::Duration;

pub(super) fn duration_from_json_timestamp(timestamp: &f64) -> Duration {
    let seconds = timestamp.round();
    let nanos = (timestamp - seconds) * 10_f64.powf(9.0);
    Duration::seconds(seconds as i64) + Duration::nanoseconds(nanos as i64)
}

#[cfg(feature = "arrow")]
pub(super) fn duration_from_arrow_timestamp_and_scale(timestamp: &i64, scale: &i64) -> Duration {
    let seconds = timestamp / scale;
    let nanos = timestamp % scale * 10_i64.pow(9) / scale;
    Duration::seconds(seconds) + Duration::nanoseconds(nanos)
}

#[cfg(feature = "arrow")]
pub(super) fn get_arrow_time_scale(field: &Field) -> Result<i64, SnowflakeError> {
    match field.metadata.get("scale") {
        Some(s) => {
            let scale = s.parse::<u32>().map_err(|e| {
                SnowflakeError::new_deserialization_error_with_field_and_value(e.into(), field.name.clone(), s.clone())
            })?;
            assert!(scale <= 9);
            Ok(10_i64.pow(scale))
        }
        None => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Missing required scale for field {}", field.name),
            field.name.clone(),
        )),
    }
}

#[cfg(feature = "arrow")]
pub(super) fn arrow_struct_to_naive_datetime(
    vec: &[Box<dyn Scalar>],
    field: &Field,
) -> Result<Option<NaiveDateTime>, SnowflakeError> {
    use arrow2::scalar::PrimitiveScalar;

    let timestamp: Option<i64> = (vec[0])
        .as_any()
        .downcast_ref::<PrimitiveScalar<i64>>()
        .and_then(|d| *d.value());

    let fraction = match vec[1].data_type() {
        DataType::Int32 => (vec[1])
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .and_then(|d| d.value().map(|d| d as i64)),
        DataType::Int64 => (vec[1])
            .as_any()
            .downcast_ref::<PrimitiveScalar<i64>>()
            .and_then(|d| *d.value()),
        x => {
            return Err(SnowflakeError::new_deserialization_error_with_field(
                anyhow!("Invalid fraction data type {:?}", x),
                field.name.clone(),
            ))
        }
    };

    if let Some(timestamp) = timestamp {
        let datetime = NaiveDateTime::from_timestamp_opt(0, 0).unwrap()
            + Duration::seconds(timestamp)
            + Duration::nanoseconds(fraction.unwrap_or(0));

        Ok(Some(datetime))
    }
    else {
        Ok(None)
    }
}
