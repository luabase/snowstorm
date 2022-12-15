use crate::errors::SnowflakeError;
use crate::responses::{row::RowType, types::ValueType};
use crate::session::Session;

use anyhow::anyhow;
use chrono::{Duration, prelude::*};
use std::collections::HashMap;

pub trait QueryDeserializer: Sized {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError>;

    fn deserialize_value(value: &serde_json::Value, row_type: &RowType) -> Result<ValueType, SnowflakeError> {
        match row_type.data_type.as_str() {
            "boolean" => {
                match value.as_bool() {
                    Some(p) => {
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::Boolean(p));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::Boolean(p))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "fixed" => {
                match value.as_str() {
                    Some(p) => {
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::Number(p.to_owned()));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::Number(p.to_owned()))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "real" => {
                match value.as_f64() {
                    Some(p) => {
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::Float(p));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::Float(p))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "text" => {
                let parsed: Option<String> = serde_json::from_value(value.to_owned())
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                match parsed {
                    Some(p) => {
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::String(p));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::String(p))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "binary" => {
                let parsed: Option<String> = serde_json::from_value(value.to_owned())
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;

                let decoded;
                match parsed {
                    Some(p) => {
                        decoded = hex::decode(p).map_err(|e| SnowflakeError::DeserializationError(e.into()));
                    },
                    None => return handle_null_value(row_type)
                }

                match decoded {
                    Ok(d) => {
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::Binary(d));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::Binary(d))
                        }
                    },
                    Err(e) => Err(e)
                }
            },
            "date" => {
                let parsed: Option<i64> = serde_json::from_value(value.clone())
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                match parsed {
                    Some(p) => {
                        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(p);
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::NaiveDate(date));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::NaiveDate(date))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "time" => {
                match value.as_f64() {
                    Some(p) => {
                        let nanos = (p * 1_000_000_000.0).round() as i64;
                        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(nanos);
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::NaiveTime(time));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::NaiveTime(time))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "timestamp_ntz" => {
                match value.as_f64() {
                    Some(p) => {
                        let nanos = (p * 1_000_000_000.0).round() as i64;
                        let date = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::NaiveDateTime(date));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::NaiveDateTime(date))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "timestamp_ltz" | "timestamp_tz" => {
                match value.as_f64() {
                    Some(p) => {
                        let nanos = (p * 1_000_000_000.0).round() as i64;
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
                    None => handle_null_value(row_type)
                }
            },
            "variant" => {
                if value.is_null() {
                    handle_null_value(row_type)
                }
                else if row_type.nullable {
                    let boxed = Box::new(ValueType::Variant(value.to_owned()));
                    Ok(ValueType::Nullable(Some(boxed)))
                }
                else {
                    Ok(ValueType::Variant(value.to_owned()))
                }
            },
            "object" => {
                let parsed: Option<HashMap<String, serde_json::Value>> = serde_json::from_value(value.to_owned())
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                match parsed {
                    Some(p) => {
                        if row_type.nullable {
                            let boxed;
                            match &row_type.ext_type_name {
                                Some(t) => {
                                    match t.as_str() {
                                        "GEOGRAPHY" => boxed = Box::new(ValueType::Geography(p)),
                                        "GEOMETRY" => boxed = Box::new(ValueType::Geometry(p)),
                                        _ => boxed = Box::new(ValueType::HashMap(p))
                                    }
                                }
                                _ => boxed = Box::new(ValueType::HashMap(p))
                            }
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::HashMap(p))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            "array" => {
                let parsed: Option<Vec<serde_json::Value>> = serde_json::from_value(value.to_owned())
                    .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
                match parsed {
                    Some(p) => {
                        if row_type.nullable {
                            let boxed = Box::new(ValueType::Vec(p));
                            Ok(ValueType::Nullable(Some(boxed)))
                        }
                        else {
                            Ok(ValueType::Vec(p))
                        }
                    },
                    None => handle_null_value(row_type)
                }
            },
            _ => {
                if value.is_null() {
                    handle_null_value(row_type)
                }
                else if row_type.nullable {
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
