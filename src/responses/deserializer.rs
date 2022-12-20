use crate::errors::SnowflakeError;
use crate::responses::types::{internal::InternalResult, row_type::RowType, value::{Value, ValueType}};

use anyhow::anyhow;
use chrono::{Duration, prelude::*};
use std::collections::HashMap;

pub trait QueryDeserializer: Sized {

    type ReturnType;

    fn deserialize_rowset(res: &InternalResult) -> Result<Self::ReturnType, SnowflakeError>;

    fn deserialize_value(value: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
        let string;
        match value.as_str() {
            Some(v) => string = v,
            None => return handle_null_value(row_type)
        }

        let value_type;
        match row_type.value_type() {
            ValueType::Nullable(v) => value_type = *v,
            _ => value_type = row_type.value_type()
        }

        match value_type {
            ValueType::Boolean => {
                let parsed = serde_json::from_str::<u8>(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                let v;
                match parsed {
                    0 => v = false,
                    1 => v = true,
                    _ => return Err(SnowflakeError::new_deserialization_error_with_field_and_value(
                        anyhow!("Unexpected boolean value {parsed}"), row_type.name.clone(), string.to_owned()
                    ))
                }

                if row_type.nullable {
                    let boxed = Box::new(Value::Boolean(v));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Boolean(v))
                }
            },
            ValueType::Number => {
                let parsed: i128 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                if row_type.nullable {
                    let boxed = Box::new(Value::Number(parsed));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Number(parsed))
                }
            },
            ValueType::Float => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                if row_type.nullable {
                    let boxed = Box::new(Value::Float(parsed));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Float(parsed))
                }
            },
            ValueType::String => {
                if row_type.nullable {
                    let boxed = Box::new(Value::String(string.to_owned()));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::String(string.to_owned()))
                }
            },
            ValueType::Binary => {
                let decoded = hex::decode(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;

                if row_type.nullable {
                    let boxed = Box::new(Value::Binary(decoded));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Binary(decoded))
                }
            },
            ValueType::NaiveDate => {
                let parsed: i64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(parsed);
                if row_type.nullable {
                    let boxed = Box::new(Value::NaiveDate(date));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::NaiveDate(date))
                }
            },
            ValueType::NaiveTime => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                let nanos = (parsed * 1_000_000_000.0).round() as i64;
                let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(nanos);
                if row_type.nullable {
                    let boxed = Box::new(Value::NaiveTime(time));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::NaiveTime(time))
                }
            },
            ValueType::NaiveDateTime => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                let nanos = (parsed * 1_000_000_000.0).round() as i64;
                let date = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                if row_type.nullable {
                    let boxed = Box::new(Value::NaiveDateTime(date));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::NaiveDateTime(date))
                }
            },
            ValueType::DateTimeUTC => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
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
            },
            ValueType::DateTime => {
                let pair = string.split_once(" ");
                let timezone_str;
                let offset_str;
                match pair {
                    Some(p) => (timezone_str, offset_str) = p,
                    None => return Err(
                        SnowflakeError::new_deserialization_error_with_field_and_value(
                            anyhow!("Expected timezone and offset pair, got {string}"),
                            row_type.name.clone(),
                            string.to_owned()
                        )
                    )
                }

                let timestamp: f64 = serde_json::from_str(timezone_str)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                let offset: i32 = serde_json::from_str(offset_str)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;

                let timezone_opt = FixedOffset::east_opt((offset - 1440) * 60);
                let timezone;
                match timezone_opt {
                    Some(tz) => timezone = tz,
                    None => return Err(
                        SnowflakeError::new_deserialization_error_with_field_and_value(
                            anyhow!("Invalid timezone offset {offset}"),
                            row_type.name.clone(),
                            string.to_owned()
                        )
                    )
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
            },
            ValueType::Variant => {
                if row_type.nullable {
                    let boxed = Box::new(Value::Variant(value.to_owned()));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Variant(value.to_owned()))
                }
            },
            ValueType::HashMap => {
                let parsed: HashMap<String, serde_json::Value> = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                if row_type.nullable {
                    let boxed;
                    match &row_type.ext_type_name {
                        Some(t) => {
                            match t.as_str() {
                                "GEOGRAPHY" => boxed = Box::new(Value::Geography(parsed)),
                                "GEOMETRY" => boxed = Box::new(Value::Geometry(parsed)),
                                _ => boxed = Box::new(Value::HashMap(parsed))
                            }
                        }
                        _ => boxed = Box::new(Value::HashMap(parsed))
                    }
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::HashMap(parsed))
                }
            },
            ValueType::Vec => {
                let parsed: Vec<serde_json::Value> = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::new_deserialization_error_with_field_and_value(
                        e.into(), row_type.name.clone(), string.to_owned()
                    ))?;
                if row_type.nullable {
                    let boxed = Box::new(Value::Vec(parsed));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::Vec(parsed))
                }
            },
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

}

pub(crate) fn handle_null_value(row_type: &RowType) -> Result<Value, SnowflakeError> {
    if row_type.nullable {
        Ok(Value::Nullable(None))
    }
    else {
        let e = anyhow!("Encountered NULL value for non-nullable field {}", row_type.name);
        Err(SnowflakeError::DeserializationError(e, None))
    }
}
