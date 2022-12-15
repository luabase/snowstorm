use chrono::prelude::*;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ValueType {
    Binary(Vec<u8>),
    Boolean(bool),
    Number(String),  // Deserialize as string, since i128 deserialization is not implemented in serde
    Float(f64),
    String(String),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
    NaiveDateTime(NaiveDateTime),
    DateTimeUTC(DateTime<Utc>),
    HashMap(HashMap<String, serde_json::Value>),
    Vec(Vec<serde_json::Value>),
    Geography(HashMap<String, serde_json::Value>),
    Geometry(HashMap<String, serde_json::Value>),
    Variant(serde_json::Value),
    Nullable(Option<Box<ValueType>>),
    Unsupported(serde_json::Value)
}
