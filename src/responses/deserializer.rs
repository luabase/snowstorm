use crate::errors::SnowflakeError;
use crate::responses::{row::RowType, types::{Value, ValueType}};
use crate::session::Session;

use anyhow::anyhow;
use chrono::{Duration, prelude::*};
use std::collections::HashMap;

pub trait QueryDeserializer: Sized {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError>;

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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                let v;
                match parsed {
                    0 => v = false,
                    1 => v = true,
                    _ => return Err(SnowflakeError::DeserializationError(anyhow!("Unexpected boolean value {parsed}")))
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| {
                        log::error!("Failed to deserialize binary.");
                        SnowflakeError::DeserializationError(e.into())
                    })?;

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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
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

    #[cfg(integer128)]
    fn serialize_value(val: &Value) -> Result<serde_json::Value, serde_json::Error> {
        match val {
            Value::Binary(v) => serde_json::to_value(&v),
            Value::Boolean(v) => serde_json::to_value(&v),
            Value::Number(x) => serde_json::to_value(&x),
            Value::Float(v) => serde_json::to_value(&v),
            Value::String(v) => serde_json::to_value(&v),
            Value::NaiveDate(v) => serde_json::to_value(&v),
            Value::NaiveTime(v) => serde_json::to_value(&v),
            Value::NaiveDateTime(v) => serde_json::to_value(&v),
            Value::DateTimeUTC(v) => serde_json::to_value(&v),
            Value::DateTime(v) => serde_json::to_value(&v),
            Value::HashMap(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Vec(v) => serde_json::to_value(&v),
            Value::Geography(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Geometry(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Variant(v) => serde_json::to_value(Self::to_json_variant(v)),
            Value::Unsupported(v) => serde_json::to_value(&v),
            Value::Nullable(v) => {
                match v {
                    Some(x) => Self::serialize_value(x),
                    None => serde_json::to_value::<Option<String>>(None)
                }
            },
        }
    }

    #[cfg(not(integer128))]
    fn serialize_value(val: &Value) -> Result<serde_json::Value, serde_json::Error> {
        match val {
            Value::Binary(v) => serde_json::to_value(&v),
            Value::Boolean(v) => serde_json::to_value(&v),
            Value::Number(x) => serde_json::to_value(&x.to_string()),
            Value::Float(v) => serde_json::to_value(&v),
            Value::String(v) => serde_json::to_value(&v),
            Value::NaiveDate(v) => serde_json::to_value(&v),
            Value::NaiveTime(v) => serde_json::to_value(&v),
            Value::NaiveDateTime(v) => serde_json::to_value(&v),
            Value::DateTimeUTC(v) => serde_json::to_value(&v),
            Value::DateTime(v) => serde_json::to_value(&v),
            Value::HashMap(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Vec(v) => serde_json::to_value(&v),
            Value::Geography(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Geometry(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Variant(v) => serde_json::to_value(Self::to_json_variant(v)),
            Value::Unsupported(v) => serde_json::to_value(&v),
            Value::Nullable(v) => {
                match v {
                    Some(x) => Self::serialize_value(x),
                    None => serde_json::to_value::<Option<String>>(None)
                }
            },
        }
    }

    fn to_json_map(m: &HashMap<String, serde_json::Value>) -> String {
        let value = serde_json::to_value(m);
        match value {
            Ok(v) => serde_json::to_string(&v).unwrap_or("{}".to_owned()),
            Err(e) => e.to_string()
        }
    }

    fn to_json_variant(v: &serde_json::Value) -> String {
        if v.is_object() {
            let str = v.as_str().unwrap_or("{}");
            let json = compacto::Compressor::new(str).unwrap().compress();
            match json {
                Ok(j) => j,
                Err(e) => e.to_string()
            }
        }
        else if v.is_array() {
            let str = v.as_str().unwrap_or("[]]");
            let json = compacto::Compressor::new(str).unwrap().compress();
            match json {
                Ok(j) => j,
                Err(e) => e.to_string()
            }
        }
        else {
            let value = serde_json::to_string(&v);
            match value {
                Ok(p) => p,
                Err(e) => e.to_string()
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

fn handle_null_value(row_type: &RowType) -> Result<Value, SnowflakeError> {
    if row_type.nullable {
        Ok(Value::Nullable(None))
    }
    else {
        let e = anyhow!("Encountered NULL value for non-nullable field {}", row_type.name);
        Err(SnowflakeError::DeserializationError(e))
    }
}
