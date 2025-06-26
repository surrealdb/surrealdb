use std::borrow::Cow;

use crate::dbs::{self, ResponseData};
use crate::dbs::{Failure, Notification};
use crate::expr;
use crate::expr::Value;
use crate::protocol::ToFlatbuffers;
use crate::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
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
				result: Err(err),
			}]),
		}
	}

	pub fn collect_errors(&self) -> Option<anyhow::Error> {
		if let ResponseData::Results(results) = &self.data {
			let mut errors = Vec::new();
			for result in results {
				if let Err(err) = &result.result {
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

impl ToFlatbuffers for Response {
	type Output<'bldr> = flatbuffers::WIPOffset<rpc_fb::Response<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let id = self.id.as_ref().map(|id| builder.create_string(&id));

		let (data_type, data) = match &self.data {
			ResponseData::Results(results) => {
				let results: Vec<flatbuffers::WIPOffset<rpc_fb::QueryResult<'_>>> =
					results.iter().map(|result| result.to_fb(builder)).collect();
				let results = builder.create_vector(&results);
				(
					rpc_fb::ResponseData::Results,
					rpc_fb::QueryResults::create(
						builder,
						&rpc_fb::QueryResultsArgs {
							results: Some(results),
						},
					)
					.as_union_value(),
				)
			}
			ResponseData::Notification(notification) => {
				let notification_fb = notification.to_fb(builder);
				(rpc_fb::ResponseData::Notification, notification_fb.as_union_value())
			}
		};

		rpc_fb::Response::create(
			builder,
			&rpc_fb::ResponseArgs {
				id,
				data_type,
				data: Some(data),
			},
		)
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
