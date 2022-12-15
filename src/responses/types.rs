use chrono::prelude::*;

pub enum ValueType {
    Binary(Vec<u8>),
    Boolean(bool),
    Numeric(i128),
    String(String),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(DateTime),
    TimestampLTZ(DateTime),
    TimestampNTZ(NaiveDateTime),
    TimestampTZ(DateTime),
    Variant(serde_json::Value),
    Object(serde_json::Value),
    Array(Vec<serde_json::Value>),
    Geography(serde_json::Value),
    Geometry(serde_json::Value)
}
