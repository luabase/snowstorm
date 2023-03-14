use crate::errors::SnowflakeError;
use crate::requests::QueryRequest;
use crate::responses::types::{
    data::DataResponse,
    error::ErrorResult,
    internal::{InternalInitAsyncQueryResult, InternalMonitoringQueriesResult, InternalResult},
    query::QueryStatus,
};
use crate::responses::{get_query_detail_url, make_chunk_downloader, QueryResult};

use anyhow::anyhow;
use backoff::backoff::Backoff;
use chrono::prelude::*;
use reqwest::header::ACCEPT;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::thread;
use std::time::Duration;
use std::{
    str,
    sync::atomic::{AtomicU32, Ordering},
};

const MAX_NO_DATA_RETRY: i32 = 24;

#[derive(Debug)]
pub struct Session {
    pub(crate) client: reqwest::Client,
    pub(crate) host: String,
    pub(crate) account: String,
    pub(crate) region: Option<String>,
    pub(crate) proxy: Option<String>,
    pub(crate) sequence_counter: AtomicU32,
}

impl Session {
    pub fn new(
        client: reqwest::Client,
        host: &str,
        account: &str,
        region: Option<&str>,
        proxy: &Option<String>,
    ) -> Self {
        Session {
            client,
            host: host.to_owned(),
            account: account.to_owned(),
            region: region.map(str::to_string),
            proxy: proxy.clone(),
            sequence_counter: AtomicU32::new(1),
        }
    }

    pub async fn execute_async<T: QueryResult + Send + Sync>(&self, query: &str) -> Result<T, SnowflakeError> {
        let init_res: InternalInitAsyncQueryResult = self.execute_query_request(query, true).await?;

        self.await_async_query(query, &init_res).await?;
        let query_id = &init_res.query_id;
        self.execute(&format!("select * from table(result_scan('{query_id}'))"))
            .await
    }

    pub async fn execute<T: QueryResult + Send + Sync>(&self, query: &str) -> Result<T, SnowflakeError> {
        let internal: InternalResult = self.execute_query_request(query, false).await?;

        self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let mut rowset;
        if let Some(r) = &internal.rowset_base64 {
            rowset = T::deserialize_rowset64(r)?;
        }
        else if let Some(r) = &internal.rowset {
            rowset = T::deserialize_rowset(r, &internal.rowtype)?;
        }
        else {
            return Err(SnowflakeError::new_deserialization_error_with_value(
                anyhow!("Missing rowsetBase64 or rowset for Arrow format"),
                format!("{:?}", internal),
            ));
        }

        if let Some(chunks) = internal.chunks.clone() {
            let downloader = make_chunk_downloader(self, &internal)?;
            for chunk in chunks {
                let loaded = match internal.query_result_format.as_str() {
                    "arrow" => chunk.load_arrow::<T>(&downloader).await,
                    "json" => chunk.load_json::<T>(&downloader, &internal.rowtype).await,
                    x => Err(SnowflakeError::ChunkLoadingError(anyhow!(
                        "Unsupported query result format {x}"
                    ))),
                }?;

                rowset.extend(&mut loaded.into_iter());
            }
        }

        Ok(T::new(&internal, &rowset, self))
    }

    async fn await_async_query(
        &self,
        query: &str,
        init_async_query_res: &InternalInitAsyncQueryResult,
    ) -> Result<(), SnowflakeError> {
        let query_id = &init_async_query_res.query_id;
        log::debug!("Awaiting async snowflake query '{query_id}'");

        let mut backoff = backoff::ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(500))
            .with_max_interval(Duration::from_secs(5))
            // Maybe set a max deadline?
            .with_max_elapsed_time(None)
            .build();

        let mut no_data_counter = 0;
        loop {
            let json = self
                .client
                .get(&self.get_monitoring_queries_url(query_id))
                // Monitoring queries uses ACCEPT - JSON. Reqwest client wont' override this.
                .header(ACCEPT, "application/json")
                .build()
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

            let body = self
                .client
                .execute(json)
                .await
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

            let text = body
                .text()
                .await
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

            let res: DataResponse<serde_json::Value> = serde_json::from_str(&text).map_err(|e| {
                log::error!("Failed to execute monitoring query {query_id} due to deserialization error.");
                SnowflakeError::new_deserialization_error_with_value(e.into(), text)
            })?;

            let monitoring_result: InternalMonitoringQueriesResult = serde_json::from_value(res.data.clone())
                .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), res.data.to_string()))?;

            let query_result = monitoring_result.queries.get(0);
            let query_status = query_result.map_or(QueryStatus::NoData, |res| res.status.clone());

            if !query_status.is_still_running() {
                if query_status == QueryStatus::Success {
                    log::debug!("Async snowflake query '{query_id}' finished.");
                    return Ok(());
                }
                else {
                    log::debug!("Async snowflake query failed with query result {:?}", query_result);
                    let error_result = query_result.map(|qr| {
                        qr.error_result
                            .to_error_result(query_id, &get_query_detail_url(self, query_id))
                    });
                    let message = query_result
                        .map(|qr| qr.error_result.error_message.clone())
                        .unwrap_or_default()
                        .unwrap_or_default();
                    return Err(SnowflakeError::ExecutionError(
                        anyhow!("Failed to execute query '{query}', with reason: {message}"),
                        error_result,
                    ));
                }
            }

            if query_status == QueryStatus::NoData {
                no_data_counter += 1;
            }

            if no_data_counter > MAX_NO_DATA_RETRY {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Cannot retrieve data on the status of this query. No information returned from server for query '{query_id}'"),
                None));
            }

            let sleep_time = match backoff.next_backoff() {
                Some(d) => d,
                None => {
                    let elapsed = backoff.get_elapsed_time();
                    return Err(SnowflakeError::ExecutionError(
                        anyhow!("Timed out waiting for async snowflake query after '{:?}'", elapsed),
                        None,
                    ));
                }
            };

            log::debug!(
                "Awaiting async snowflake query... Status is {query_status}. Sleeping for {:?}...",
                sleep_time
            );
            thread::sleep(sleep_time);
        }
    }

    async fn execute_query_request<T: DeserializeOwned>(
        &self,
        query: &str,
        async_exec: bool,
    ) -> Result<T, SnowflakeError> {
        let now = Utc::now();
        let req = QueryRequest {
            async_exec: async_exec,
            parameters: Some(json!({ "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": Self::result_format() })),
            query_submission_time: now.timestamp_millis(),
            sequence_id: self.sequence_counter.load(Ordering::Relaxed),
            sql_text: query,
        };

        let json = self
            .client
            .post(&self.get_queries_url("query-request"))
            .json(&req)
            .build()
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let body = self
            .client
            .execute(json)
            .await
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let text = body
            .text()
            .await
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let res: DataResponse<serde_json::Value> = serde_json::from_str(&text).map_err(|e| {
            log::error!("Failed to execute query {query} due to deserialization error.");
            SnowflakeError::new_deserialization_error_with_value(e.into(), text)
        })?;

        if !res.success {
            let err = ErrorResult::deserialize(res.data, self)?;
            if let Some(message) = res.message {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query} with reason: {message}"),
                    Some(err),
                ));
            }
            else {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query}, but no reason was given by Snowflake API"),
                    Some(err),
                ));
            }
        }

        let parsed: T = serde_json::from_value(res.data.clone())
            .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), res.data.to_string()))?;
        Ok(parsed)
    }

    fn get_queries_url(&self, command: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        let guid = uuid::Uuid::new_v4();
        let url = format!(
            "{}/queries/v1/{command}?requestId={uuid}&request_guid={guid}",
            self.host
        );
        log::debug!("Using query url {url}");
        url
    }

    fn get_monitoring_queries_url(&self, query_id: &String) -> String {
        let encoded_query_id = url_escape::encode_fragment(query_id);
        let url = format!("{}/monitoring/queries/{encoded_query_id}", self.host);
        log::debug!("Using query monitoring {url}");
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
