use std::fmt::Debug;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TestError {
	#[error("A network error occurred: {message}")]
	NetworkError {
		message: String,
	},

	#[error("An assertion failed as part of an invocation stack: {message}")]
	AssertionError {
		message: String,
	},
}
