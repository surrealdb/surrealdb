use std::borrow::Cow;

use crate::dbs::Notification;
use crate::dbs::{self, QueryResultData};
use crate::expr;
use crate::expr::Value;
use revision::revisioned;
use serde::Serialize;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct Response {
	pub id: String,
	pub result: Result<QueryResultData, Failure>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Failure {
	pub(crate) code: i64,
	pub(crate) message: Cow<'static, str>,
}

impl Error for Failure {}

impl fmt::Display for Failure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Failure ({}): {}", self.code, self.message)
	}
}
