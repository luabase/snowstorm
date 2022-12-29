use crate::errors::SnowflakeError;
use crate::responses::QueryResult;
use crate::responses::types::row_type::RowType;
use serde::Deserialize;

use async_compression::futures::bufread::GzipDecoder;
use futures::{io::{self, BufReader, ErrorKind}, prelude::*,};
use reqwest::header::CONTENT_ENCODING;


#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub row_count: u64,
    pub url: String
}

impl Chunk {

    pub(crate) async fn load<T: QueryResult>(
        &self,
        client: &reqwest::Client,
        rowtype: &[RowType]
    ) -> Result<Vec<T::ReturnType>, SnowflakeError> {

        let req = client
            .get(&self.url)
            .build()
            .map_err(|e| SnowflakeError::ChunkLoadingError(e.into()))?;

        let body = client
            .execute(req).await
            .map_err(|e| SnowflakeError::ChunkLoadingError(e.into()))?;

        let headers = body.headers();
        let mut should_decompress = false;
        if let Some(x) = headers.get(CONTENT_ENCODING) {
            if x.to_str().unwrap_or("").to_ascii_lowercase() == "gzip" {
                should_decompress = true
            }
        }

        let text;
        if should_decompress {
            let reader = body
                .bytes_stream()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))
                .into_async_read();
            let mut decoder = GzipDecoder::new(BufReader::new(reader));
            let mut data = String::new();
            decoder.read_to_string(&mut data).await
                .map_err(|e| SnowflakeError::ChunkLoadingError(e.into()))?;
            text = "[".to_owned() + &data + "]";
        }
        else {
            text = body
                .text().await
                .map_err(|e| SnowflakeError::ChunkLoadingError(e.into()))?;
        }

        let res: Vec<Vec<serde_json::Value>> = serde_json::from_str(&text)
            .map_err(|e| {
                log::error!("Failed to load chunk due to deserialization error.");
                SnowflakeError::new_deserialization_error_with_value(e.into(), text)
            })?;

        let data = T::deserialize_rowset(&res, rowtype)
            .map_err(|e| {
                log::error!(
                    "Failed to load chunk due to data deserialization error."
                );
                e
            })?;

        Ok(data)
    }

}
