use crate::errors::SnowflakeError;
use crate::responses::{row::RowType, types::ValueType};
use crate::session::Session;

use anyhow::anyhow;
use chrono::{Duration, prelude::*};
use std::collections::HashMap;

pub trait QueryDeserializer: Sized {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError>;

    fn deserialize_value(value: &serde_json::Value, row_type: &RowType) -> Result<ValueType, SnowflakeError> {
        let string;
        match value.as_str() {
            Some(v) => string = v,
            None => return handle_null_value(row_type)
        }

        match row_type.data_type.as_str() {
            "boolean" => {
                let parsed = serde_json::from_str::<u8>(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let v;
                match parsed {
                    0 => v = false,
                    1 => v = true,
                    _ => return Err(SnowflakeError::DeserializationError(anyhow!("Unexpected boolean value {parsed}")))
                }

                if row_type.nullable {
                    let boxed = Box::new(ValueType::Boolean(v));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Boolean(v))
                }
            },
            "fixed" => {
                let parsed: i128 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                if row_type.nullable {
                    let boxed = Box::new(ValueType::Number(parsed));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Number(parsed))
                }
            },
            "real" => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                if row_type.nullable {
                    let boxed = Box::new(ValueType::Float(parsed));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Float(parsed))
                }
            },
            "text" => {
                if row_type.nullable {
                    let boxed = Box::new(ValueType::String(string.to_owned()));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::String(string.to_owned()))
                }
            },
            "binary" => {
                let decoded = hex::decode(string)
                    .map_err(|e| {
                        log::error!("Failed to deserialize binary.");
                        SnowflakeError::DeserializationError(e.into())
                    })?;

                if row_type.nullable {
                    let boxed = Box::new(ValueType::Binary(decoded));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Binary(decoded))
                }
            },
            "date" => {
                let parsed: i64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(parsed);
                if row_type.nullable {
                    let boxed = Box::new(ValueType::NaiveDate(date));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::NaiveDate(date))
                }
            },
            "time" => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let nanos = (parsed * 1_000_000_000.0).round() as i64;
                let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(nanos);
                if row_type.nullable {
                    let boxed = Box::new(ValueType::NaiveTime(time));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::NaiveTime(time))
                }
            },
            "timestamp_ntz" => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let nanos = (parsed * 1_000_000_000.0).round() as i64;
                let date = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                if row_type.nullable {
                    let boxed = Box::new(ValueType::NaiveDateTime(date));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::NaiveDateTime(date))
                }
            },
            "timestamp_ltz" => {
                let parsed: f64 = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let nanos = (parsed * 1_000_000_000.0).round() as i64;
                let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                let datetime = DateTime::<Utc>::from_utc(naive, Utc);

                if row_type.nullable {
                    let boxed = Box::new(ValueType::DateTimeUTC(datetime));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::DateTimeUTC(datetime))
                }
            },
            "timestamp_tz" => {
                let pair = string.split_once(" ");
                let timezone_str;
                let offset_str;
                match pair {
                    Some(p) => (timezone_str, offset_str) = p,
                    None => return Err(
                        SnowflakeError::DeserializationError(anyhow!("Expected timezone and offset pair, got {string}"))
                    )
                }

                let timestamp: f64 = serde_json::from_str(timezone_str)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let offset: i32 = serde_json::from_str(offset_str)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;

                let timezone_opt = FixedOffset::east_opt((offset - 1440) * 60);
                let timezone;
                match timezone_opt {
                    Some(tz) => timezone = tz,
                    None => return Err(
                        SnowflakeError::DeserializationError(anyhow!("Invalid timezone offset {offset}"))
                    )
                }

                let nanos = (timestamp * 1_000_000_000.0).round() as i64;
                let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                let datetime = DateTime::<FixedOffset>::from_local(naive, timezone);

                if row_type.nullable {
                    let boxed = Box::new(ValueType::DateTime(datetime));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::DateTime(datetime))
                }
            },
            "variant" => {
                if row_type.nullable {
                    let boxed = Box::new(ValueType::Variant(value.to_owned()));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Variant(value.to_owned()))
                }
            },
            "object" => {
                let parsed: HashMap<String, serde_json::Value> = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                if row_type.nullable {
                    let boxed;
                    match &row_type.ext_type_name {
                        Some(t) => {
                            match t.as_str() {
                                "GEOGRAPHY" => boxed = Box::new(ValueType::Geography(parsed)),
                                "GEOMETRY" => boxed = Box::new(ValueType::Geometry(parsed)),
                                _ => boxed = Box::new(ValueType::HashMap(parsed))
                            }
                        }
                        _ => boxed = Box::new(ValueType::HashMap(parsed))
                    }
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::HashMap(parsed))
                }
            },
            "array" => {
                let parsed: Vec<serde_json::Value> = serde_json::from_str(string)
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                if row_type.nullable {
                    let boxed = Box::new(ValueType::Vec(parsed));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Vec(parsed))
                }
            },
            _ => {
                if row_type.nullable {
                    let boxed = Box::new(ValueType::Unsupported(value.to_owned()));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Unsupported(value.to_owned()))
                }
            }
        }
    }

    fn get_query_detail_url(session: &Session, query_id: &String) -> String {
        let components: Vec<String> = [session.region.clone(), Some(session.account.clone())]
            .into_iter()
            .filter_map(|x| x)
            .collect();
        let path = components.join("/");
        format!("https://app.snowflake.com/{path}/#/compute/history/queries/{query_id}/detail")
    }

}

fn handle_null_value(row_type: &RowType) -> Result<ValueType, SnowflakeError> {
    if row_type.nullable {
        Ok(ValueType::Nullable(None))
    }
    else {
        let e = anyhow!("Encountered NULL value for non-nullable field {}", row_type.name);
        Err(SnowflakeError::DeserializationError(e))
    }
}
