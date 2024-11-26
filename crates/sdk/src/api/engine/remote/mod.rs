//! Protocols for communicating with the server

#[cfg(feature = "protocol-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-http")))]
pub mod http;

#[cfg(feature = "protocol-ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-ws")))]
pub mod ws;

use crate::api::{self, conn::DbResponse, err::Error, method::query::QueryResult, Result};
use crate::dbs::{self, Status};
use crate::method::Stats;
use indexmap::IndexMap;
use revision::revisioned;
use revision::Revisioned;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::time::Duration;
use surrealdb_core::sql::Value as CoreValue;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;

pub struct WsNotification {}

// Converts a debug representation of `std::time::Duration` back
fn duration_from_str(duration: &str) -> Option<std::time::Duration> {
	let nanos = if let Some(duration) = duration.strip_suffix("ns") {
		duration.parse().ok()?
	} else if let Some(duration) = duration.strip_suffix("µs") {
		let micros = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_MICRO, 0).ok()?;
		micros.checked_mul(multiplier)?.to_u128()?
	} else if let Some(duration) = duration.strip_suffix("ms") {
		let millis = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_MILLI, 0).ok()?;
		millis.checked_mul(multiplier)?.to_u128()?
	} else {
		let duration = duration.strip_suffix('s')?;
		let secs = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_SEC, 0).ok()?;
		secs.checked_mul(multiplier)?.to_u128()?
	};
	let secs = nanos.checked_div(NANOS_PER_SEC as u128)?;
	let nanos = nanos % (NANOS_PER_SEC as u128);
	Some(Duration::new(secs.try_into().ok()?, nanos.try_into().ok()?))
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	#[test]
	fn duration_from_str() {
		let durations = vec![
			Duration::ZERO,
			Duration::from_nanos(1),
			Duration::from_nanos(u64::MAX),
			Duration::from_micros(1),
			Duration::from_micros(u64::MAX),
			Duration::from_millis(1),
			Duration::from_millis(u64::MAX),
			Duration::from_secs(1),
			Duration::from_secs(u64::MAX),
			Duration::MAX,
		];

		for duration in durations {
			let string = format!("{duration:?}");
			let parsed = super::duration_from_str(&string)
				.unwrap_or_else(|| panic!("Duration {string} failed to parse"));
			assert_eq!(duration, parsed, "Duration {string} not parsed correctly");
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Failure {
	pub(crate) code: i64,
	pub(crate) message: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
pub(crate) enum Data {
	Other(CoreValue),
	Query(Vec<dbs::QueryMethodResponse>),
	Live(dbs::Notification),
}

type ServerResult = std::result::Result<Data, Failure>;

impl From<Failure> for Error {
	fn from(failure: Failure) -> Self {
		match failure.code {
			-32600 => Self::InvalidRequest(failure.message),
			-32602 => Self::InvalidParams(failure.message),
			-32603 => Self::InternalError(failure.message),
			-32700 => Self::ParseError(failure.message),
			_ => Self::Query(failure.message),
		}
	}
}

impl From<Failure> for crate::Error {
	fn from(value: Failure) -> Self {
		let api_err: Error = value.into();
		api_err.into()
	}
}

impl DbResponse {
	fn from_server_result(result: ServerResult) -> Result<Self> {
		match result.map_err(Error::from)? {
			Data::Other(value) => Ok(DbResponse::Other(value)),
			Data::Query(responses) => {
				let mut map =
					IndexMap::<usize, (Stats, QueryResult)>::with_capacity(responses.len());

				for (index, response) in responses.into_iter().enumerate() {
					let stats = Stats {
						execution_time: duration_from_str(&response.time),
					};
					match response.status {
						Status::Ok => {
							map.insert(index, (stats, Ok(response.result)));
						}
						Status::Err => {
							map.insert(
								index,
								(stats, Err(Error::Query(response.result.as_raw_string()).into())),
							);
						}
						_ => unreachable!(),
					}
				}

				Ok(DbResponse::Query(api::Response {
					results: map,
					..api::Response::new()
				}))
			}
			// Live notifications don't call this method
			Data::Live(..) => unreachable!(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
pub(crate) struct Response {
	id: Option<CoreValue>,
	pub(crate) result: ServerResult,
}

fn serialize<V>(value: &V, revisioned: bool) -> Result<Vec<u8>>
where
	V: serde::Serialize + Revisioned,
{
	if revisioned {
		let mut buf = Vec::new();
		value.serialize_revisioned(&mut buf).map_err(|error| crate::Error::Db(error.into()))?;
		return Ok(buf);
	}
	surrealdb_core::sql::serde::serialize(value).map_err(|error| crate::Error::Db(error.into()))
}

fn deserialize<T>(bytes: &[u8], revisioned: bool) -> Result<T>
where
	T: Revisioned + DeserializeOwned,
{
	if revisioned {
		let mut read = std::io::Cursor::new(bytes);
		return T::deserialize_revisioned(&mut read).map_err(|x| crate::Error::Db(x.into()));
	}
	surrealdb_core::sql::serde::deserialize(bytes).map_err(|error| crate::Error::Db(error.into()))
}
