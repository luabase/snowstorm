pub mod deserializer;
pub mod result;
pub mod serializer;
pub mod types;

use crate::errors::SnowflakeError;
use crate::responses::types::internal::InternalResult;
use crate::session::Session;

use anyhow::anyhow;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use std::{collections::HashMap, time::Duration};

#[async_trait]
pub trait QueryResult: deserializer::QueryDeserializer + serializer::QuerySerializer + Sized {
    fn new(res: &InternalResult, rowset: &[Self::ReturnType], session: &Session) -> Self;
}

pub(crate) fn get_query_detail_url(session: &Session, query_id: &String) -> String {
    let components: Vec<String> = [session.region.clone(), Some(session.account.clone())]
        .into_iter()
        .flatten()
        .collect();
    let path = components.join("/");
    format!("https://app.snowflake.com/{path}/#/compute/history/queries/{query_id}/detail")
}

pub(self) fn default_chunk_headers(encryption_key: &str) -> Result<HeaderMap, anyhow::Error> {
    let mut headers = HeaderMap::with_capacity(3);
    headers.append(
        USER_AGENT,
        concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION")).parse()?,
    );
    headers.append(
        "x-amz-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.append("x-amz-server-side-encryption-customer-key", encryption_key.parse()?);
    Ok(headers)
}

pub(crate) fn make_chunk_headers(raw_headers: &HashMap<String, serde_json::Value>) -> Result<HeaderMap, anyhow::Error> {
    let mut headers = HeaderMap::with_capacity(raw_headers.len());
    for (k, v) in raw_headers.iter() {
        let name = HeaderName::from_bytes(k.as_bytes()).unwrap();
        let value: String = serde_json::from_value(v.clone())?;
        headers.insert(name, value.parse()?);
    }
    Ok(headers)
}

pub(super) fn make_chunk_downloader(
    session: &Session,
    res: &InternalResult,
    timeout: Option<Duration>,
) -> Result<reqwest::Client, SnowflakeError> {
    let headers = match &res.chunk_headers {
        Some(h) => make_chunk_headers(h).map_err(SnowflakeError::ChunkLoadingError)?,
        None => match &res.qrmk {
            Some(k) => default_chunk_headers(k.as_str()).map_err(SnowflakeError::ChunkLoadingError)?,
            None => return Err(SnowflakeError::ChunkLoadingError(anyhow!("Encryption key is missing"))),
        },
    };

    let mut builder = reqwest::Client::builder()
        .gzip(true)
        .deflate(true)
        .default_headers(headers);

    if let Some(dur) = timeout {
        builder = builder.timeout(dur);
    }

    if let Some(proxy) = &session.proxy {
        builder = builder.proxy(reqwest::Proxy::https(proxy).unwrap());
    }

    builder.build().map_err(|e| SnowflakeError::GeneralError(e.into()))
}
