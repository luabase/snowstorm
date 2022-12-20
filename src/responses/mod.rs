pub mod deserializer;
pub mod result;
pub mod serializer;
pub mod types;

use crate::errors::SnowflakeError;
use crate::responses::types::{chunk::Chunk, internal::InternalResult};
use crate::session::Session;

use anyhow::anyhow;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use std::collections::HashMap;


#[async_trait]
pub trait QueryResult: deserializer::QueryDeserializer + serializer::QuerySerializer + Sized {

    fn new(res: &InternalResult, rowset: &Vec<Self::ReturnType>, session: &Session) -> Self;

    async fn load_chunk(res: &InternalResult, chunk: &Chunk) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        let headers;
        match &res.chunk_headers {
            Some(h) => headers = make_chunk_headers(&h).map_err(SnowflakeError::ChunkLoadingError)?,
            None => {
                match &res.qrmk {
                    Some(k) => headers = default_chunk_headers(k.as_str()).map_err(SnowflakeError::ChunkLoadingError)?,
                    None => return Err(SnowflakeError::ChunkLoadingError(anyhow!("Encryption key is missing")))
                }
            }
        }

        let client = reqwest::Client::builder()
            .gzip(true)
            .deflate(true)
            .default_headers(headers)
            .build()
            .map_err(|e| SnowflakeError::GeneralError(e.into()))?;

        let res = chunk.load::<Self>(&client, &res.rowtype).await?;
        Ok(res)
    }

}

pub(crate) fn get_query_detail_url(session: &Session, query_id: &String) -> String {
    let components: Vec<String> = [session.region.clone(), Some(session.account.clone())]
        .into_iter()
        .filter_map(|x| x)
        .collect();
    let path = components.join("/");
    format!("https://app.snowflake.com/{path}/#/compute/history/queries/{query_id}/detail")
}

pub(self) fn default_chunk_headers(encryption_key: &str) -> Result<HeaderMap, anyhow::Error> {
    let mut headers = HeaderMap::with_capacity(3);
    headers.append(USER_AGENT, concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION")).parse()?);
    headers.append("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
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
