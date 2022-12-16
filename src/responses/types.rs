use chrono::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Value {
    Binary(Vec<u8>),
    Boolean(bool),
    Number(i128),
    Float(f64),
    String(String),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
    NaiveDateTime(NaiveDateTime),
    DateTimeUTC(DateTime<Utc>),
    DateTime(DateTime<FixedOffset>),
    HashMap(HashMap<String, serde_json::Value>),
    Vec(Vec<serde_json::Value>),
    Geography(HashMap<String, serde_json::Value>),
    Geometry(HashMap<String, serde_json::Value>),
    Variant(serde_json::Value),
    Nullable(Option<Box<Value>>),
    Unsupported(serde_json::Value)
}

impl fmt::Display for Value {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Binary(v) => write!(f, "{:?}", *v),
            Value::Boolean(v) => write!(f, "{:?}", *v),
            Value::Number(v) => write!(f, "{:?}", *v),
            Value::Float(v) => write!(f, "{:?}", *v),
            Value::String(v) => write!(f, "{}", *v),
            Value::NaiveDate(v) => write!(f, "{:?}", *v),
            Value::NaiveTime(v) => write!(f, "{:?}", *v),
            Value::NaiveDateTime(v) => write!(f, "{:?}", *v),
            Value::DateTimeUTC(v) => write!(f, "{:?}", *v),
            Value::DateTime(v) => write!(f, "{:?}", *v),
            Value::HashMap(v) => write!(f, "{:?}", to_pretty_map(v)),
            Value::Vec(v) => write!(f, "{:?}", *v),
            Value::Geography(v) => write!(f, "{:?}", to_pretty_map(v)),
            Value::Geometry(v) => write!(f, "{:?}", to_pretty_map(v)),
            Value::Variant(v) => write!(f, "{:?}", serde_json::to_string_pretty(v).unwrap_or("{}".to_owned())),
            Value::Unsupported(v) => write!(f, "{:?}", *v),
            Value::Nullable(b) => {
                match b {
                    Some(v) => write!(f, "{}", *v),
                    None => write!(f, "NULL"),
                }
            }
        }
    }

}

fn to_pretty_map(m: &HashMap<String, serde_json::Value>) -> String {
    let value = serde_json::to_value(m);
    match value {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or("{}".to_owned()),
        Err(e) => e.to_string()
    }
}

#[derive(Debug, Serialize)]
pub enum ValueType {
    Binary,
    Boolean,
    Number,
    Float,
    String,
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
    DateTimeUTC,
    DateTime,
    HashMap,
    Vec,
    Geography,
    Geometry,
    Variant,
    Nullable(Box<ValueType>),
    Unsupported
}
