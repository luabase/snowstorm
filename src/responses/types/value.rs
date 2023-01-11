use chrono::prelude::*;
use decimal_rs::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum Value {
    Binary(Vec<u8>),
    Boolean(bool),
    Decimal(Decimal),
    Integer(i128),
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
    Unsupported(serde_json::Value),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Binary(v) => write!(f, "{:?}", *v),
            Value::Boolean(v) => write!(f, "{:?}", *v),
            Value::Decimal(v) => write!(f, "{:?}", *v),
            Value::Integer(v) => write!(f, "{:?}", *v),
            Value::Float(v) => write!(f, "{:?}", *v),
            Value::String(v) => write!(f, "{}", *v),
            Value::NaiveDate(v) => write!(f, "{:?}", *v),
            Value::NaiveTime(v) => write!(f, "{:?}", *v),
            Value::NaiveDateTime(v) => write!(f, "{:?}", *v),
            Value::DateTimeUTC(v) => write!(f, "{:?}", *v),
            Value::DateTime(v) => write!(f, "{:?}", *v),
            Value::HashMap(v) => write!(f, "{:?}", *v),
            Value::Vec(v) => write!(f, "{:?}", *v),
            Value::Geography(v) => write!(f, "{:?}", *v),
            Value::Geometry(v) => write!(f, "{:?}", *v),
            Value::Variant(v) => write!(f, "{:?}", *v),
            Value::Unsupported(v) => write!(f, "{:?}", *v),
            Value::Nullable(b) => match b {
                Some(v) => write!(f, "{}", *v),
                None => write!(f, "NULL"),
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub enum ValueType {
    Binary,
    Boolean,
    Decimal,
    Integer,
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
    Unsupported,
}
