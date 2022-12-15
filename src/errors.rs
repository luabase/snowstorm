use crate::responses::error::ErrorResult;

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error("Snowflake authentication error: {0:?}")]
    AuthenticationError(anyhow::Error),
    #[error("Snowflake deserialization error: {0:?}")]
    DeserializationError(anyhow::Error),
    #[error("Snowflake execution error: {0:?}")]
    ExecutionError(anyhow::Error, Option<ErrorResult>),
    #[error("Snowflake error: {0:?}")]
    GeneralError(anyhow::Error),
}
