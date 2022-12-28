use crate::responses::types::error::ErrorResult;

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error("Snowflake authentication error: {0:?}")]
    AuthenticationError(anyhow::Error),
    #[error("Chunk loading error: {0:?}")]
    ChunkLoadingError(anyhow::Error),
    #[error("Serialization error: {0:?}")]
    SerializationError(anyhow::Error),
    #[error("Snowflake deserialization error: {0:?} {1:?}")]
    DeserializationError(anyhow::Error, Option<DeserializationErrorContext>),
    #[error("Snowflake execution error: {0:?}")]
    ExecutionError(anyhow::Error, Option<ErrorResult>),
    #[error("Snowflake error: {0:?}")]
    GeneralError(anyhow::Error),
}

impl SnowflakeError {

    pub(crate) fn new_deserialization_error(err: anyhow::Error) -> Self {
        Self::DeserializationError(err, None)
    }

    pub(crate) fn new_deserialization_error_with_value(err: anyhow::Error, value: String) -> Self {
        Self::DeserializationError(err, Some(DeserializationErrorContext {
            field: None,
            value: Some(value)
        }))
    }

    pub(crate) fn new_deserialization_error_with_field_and_value(
        err: anyhow::Error,
        field: String,
        value: String
    ) -> Self {
        Self::DeserializationError(err, Some(DeserializationErrorContext {
            field: Some(field),
            value: Some(value)
        }))
    }

}

#[derive(Debug)]
pub struct DeserializationErrorContext {
    pub field: Option<String>,
    pub value: Option<String>
}
