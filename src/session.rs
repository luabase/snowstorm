use crate::errors::SnowflakeError;
use crate::responses::QueryResult;
use crate::responses::types::{data::DataResponse, error::ErrorResult, internal::InternalResult};
use crate::requests::QueryRequest;

use anyhow::anyhow;
use chrono::prelude::*;
use serde_json::json;
use std::{str, cell::Cell};

#[derive(Debug)]
pub struct Session {
    pub(crate) client: reqwest::Client,
    pub(crate) host: String,
    pub(crate) account: String,
    pub(crate) region: Option<String>,
    pub(crate) sequence_counter: Cell<u32>
}

impl Session {

    pub fn new(client: reqwest::Client, host: &str, account: &str, region: Option<&str>) -> Self {
        Session {
            client,
            host: host.to_owned(),
            account: account.to_owned(),
            region: region.map(str::to_string),
            sequence_counter: Cell::new(1)
        }
    }

    pub async fn execute<T: QueryResult + Send + Sync>(&self, query: &str) -> Result<T, SnowflakeError> {
        let now = Utc::now();
        let req = QueryRequest {
            async_exec: false,
            parameters: Some(json!({
                "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": Self::result_format()
            })),
            query_submission_time: now.timestamp_millis(),
            sequence_id: self.sequence_counter.get(),
            sql_text: query
        };

        self.sequence_counter.set(self.sequence_counter.get() + 1);

        let json = self.client
            .post(&self.get_queries_url("query-request"))
            .json(&req)
            .build()
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let body = self.client
            .execute(json).await
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let text = body
            .text().await
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let res: DataResponse<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| {
                log::error!("Failed to execute query {query} due to deserialization error.");
                SnowflakeError::new_deserialization_error_with_value(e.into(), text)
            })?;

        if !res.success {
            let err = ErrorResult::deserialize(res.data, self)?;
            if let Some(message) = res.message {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query} with reason: {message}"),
                    Some(err)
                ));
            }
            else {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query}, but no reason was given by Snowflake API"),
                    Some(err)
                ));
            }
        }

        let internal: InternalResult = serde_json::from_value(res.data.clone())
            .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), res.data.to_string()))?;

        let mut rowset;
        if let Some(r) = &internal.rowset_base64 {
            if cfg!(feature = "arrow") {
                rowset = T::deserialize_rowset64(&r)?;
            }
            else {
                return Err(
                    SnowflakeError::GeneralError(
                        anyhow!("Cannot deserialize Arrow format since arrow feature is disabled")
                    )
                );
            }
        }
        else if let Some(r) = &internal.rowset {
            rowset = T::deserialize_rowset(&r, &internal.rowtype)?;
        }
        else {
            return Err(
                SnowflakeError::new_deserialization_error_with_value(
                    anyhow!("Missing rowsetBase64 or rowset for Arrow format"),
                    res.data.to_string()
                )
            );
        }

        if let Some(chunks) = internal.chunks.clone() {
            for chunk in chunks {
                let loaded: Vec<T::ReturnType> = T::load_chunk(&internal, &chunk).await?;
                rowset.extend(&mut loaded.into_iter());
            }
        }

        Ok(T::new(&internal, &rowset, self))
    }

    fn get_queries_url(&self, command: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        let guid = uuid::Uuid::new_v4();
        let url = format!("{}/queries/v1/{command}?requestId={uuid}&request_guid={guid}", self.host);
        log::debug!("Using query url {url}");
        url
    }

    fn result_format() -> &'static str {
        if cfg!(feature = "arrow") {
            "ARROW"
        }
        else {
            "JSON"
        }
    }
}
