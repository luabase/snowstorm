use serde::Deserialize;
use strum_macros::Display;

#[derive(Clone, Display, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    Running,
    Aborting,
    Success,
    FailedWithError,
    Aborted,
    Queued,
    FailedWithIncident,
    Disconnected,
    ResumingWarehouse,
    QueuedReparingWarehouse,
    Restarted,
    Blocked,
    NoData,
}

impl QueryStatus {
    pub fn is_still_running(&self) -> bool {
        match self {
            QueryStatus::Running
            | QueryStatus::Queued
            | QueryStatus::ResumingWarehouse
            | QueryStatus::QueuedReparingWarehouse
            | QueryStatus::Blocked
            | QueryStatus::NoData => true,
            _ => false,
        }
    }

    pub fn is_an_error(&self) -> bool {
        match self {
            QueryStatus::Aborting
            | QueryStatus::FailedWithError
            | QueryStatus::Aborted
            | QueryStatus::FailedWithIncident
            | QueryStatus::Disconnected => true,
            _ => false,
        }
    }
}
