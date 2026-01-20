//! Custom error that can be attached to a web framework to automcatically result in a http response,
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

#[macro_export]
macro_rules! safe_eject {
    // Match when the optional string is provided
    ($e:expr, $err_status:expr, $msg:expr) => {
        $e.map_err(|x| {
            let file_track = format!("{}:{}", file!(), line!());
            let formatted_error = format!("{} => {}", file_track, x.to_string());
            SurrealError::new(formatted_error, $err_status)
        })?
    };
    // Match when the optional string is not provided
    ($e:expr, $err_status:expr) => {
        $e.map_err(|x| {
            let file_track = format!("{}:{}", file!(), line!());
            let formatted_error = format!("{} => {}", file_track, x.to_string());
            SurrealError::new(formatted_error, $err_status)
        })?
    };
}

#[macro_export]
macro_rules! safe_eject_internal {
    // Match when the optional string is provided
    ($e:expr, $err_status:expr, $msg:expr) => {
        $e.map_err(|x| {
            let file_track = format!("{}:{}", file!(), line!());
            let formatted_error = format!("{} => {}", file_track, x.to_string());
            SurrealError::new(formatted_error, SurrealErrorStatus::Unknown)
        })?
    };
    // Match when the optional string is not provided
    ($e:expr) => {
        $e.map_err(|x| {
            let file_track = format!("{}:{}", file!(), line!());
            let formatted_error = format!("{} => {}", file_track, x.to_string());
            SurrealError::new(formatted_error, SurrealErrorStatus::Unknown)
        })?
    };
}

#[macro_export]
macro_rules! safe_eject_option {
    ($check:expr) => {
        match $check {
            Some(x) => x,
            None => {
                let file_track = format!("{}:{}", file!(), line!());
                let message = format!("{}=>The value is not found", file_track);
                return Err(SurrealError::new(message, SurrealErrorStatus::NotFound));
            }
        }
    };
}

/// The status of the custom error.
///
/// # Fields
/// * `NotFound` - The request was not found.
/// * `Forbidden` - You are forbidden to access.
/// * `Unknown` - An unknown internal error occurred.
/// * `BadRequest` - The request was bad.
/// * `Conflict` - The request conflicted with the current state of the server.
#[derive(Error, Debug, Serialize, Deserialize, PartialEq)]
pub enum SurrealErrorStatus {
    #[error("not found")]
    NotFound,
    #[error("You are forbidden to access resource")]
    Forbidden,
    #[error("Unknown Internal Error")]
    Unknown,
    #[error("Bad Request")]
    BadRequest,
    #[error("Conflict")]
    Conflict,
    #[error("Unauthorized")]
    Unauthorized,
}

/// The custom error that the web framework will construct into a HTTP response.
///
/// # Fields
/// * `message` - The message of the error.
/// * `status` - The status of the error.
#[derive(Serialize, Deserialize, Debug, Error)]
pub struct SurrealError {
    pub message: String,
    pub status: SurrealErrorStatus,
}

impl SurrealError {
    /// Create a new custom error.
    ///
    /// # Arguments
    /// * `message` - The message of the error.
    /// * `status` - The status of the error.
    ///
    /// # Returns
    /// A new custom error.
    pub fn new(message: String, status: SurrealErrorStatus) -> Self {
        SurrealError { message, status }
    }
}

impl fmt::Display for SurrealError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
