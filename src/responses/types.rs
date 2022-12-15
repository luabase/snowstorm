use chrono::prelude::*;

pub enum ValueType {
    Binary(Vec<u8>),
    Boolean(bool),
    Number(i128),
    Float(f64),
    String(String),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(DateTime<Utc>),
    TimestampLTZ(DateTime<Utc>),
    TimestampNTZ(NaiveDateTime),
    TimestampTZ(DateTime<Utc>),
    Variant(serde_json::Value),
    Object(serde_json::Value),
    Array(Vec<serde_json::Value>),
    Geography(serde_json::Value),
    Geometry(serde_json::Value),
    Unsupported(serde_json::Value)
}
