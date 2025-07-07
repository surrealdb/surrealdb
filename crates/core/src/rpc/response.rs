use std::borrow::Cow;

use crate::dbs::{self, ResponseData};
use crate::dbs::{Failure, Notification};
use crate::expr;
use crate::expr::Value;
use crate::protocol::ToFlatbuffers;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
	pub id: Option<String>,
	pub data: ResponseData,
}

impl Response {
	pub fn success(id: Option<String>, data: impl Into<ResponseData>) -> Self {
		Self {
			id,
			data: data.into(),
		}
	}

	pub fn failure(id: Option<String>, err: Failure) -> Self {
		Self {
			id,
			data: ResponseData::Results(vec![dbs::QueryResult {
				stats: dbs::QueryStats::default(),
				values: Err(err),
			}]),
		}
	}

	pub fn collect_errors(&self) -> Option<anyhow::Error> {
		if let ResponseData::Results(results) = &self.data {
			let mut errors = Vec::new();
			for result in results {
				if let Err(err) = &result.values {
					errors.push(err.clone());
				}
			}
			if !errors.is_empty() {
				return Some(anyhow::anyhow!("Errors occurred: {:?}", errors));
			}
		}
		None
	}
}

pub trait IntoRpcResponse {
	fn into_response(self, id: Option<String>) -> Response;
}

impl<T, E> IntoRpcResponse for Result<T, E>
where
	T: Into<ResponseData>,
	E: Into<Failure>,
{
	fn into_response(self, id: Option<String>) -> Response {
		match self {
			Ok(v) => Response::success(id, v.into()),
			Err(err) => Response::failure(id, err.into()),
		}
	}
}
