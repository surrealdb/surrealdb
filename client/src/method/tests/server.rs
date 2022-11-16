use super::types::Credentials;
use super::types::User;
use crate::method::Method;
use crate::param::from_json;
use crate::param::from_value;
use crate::param::DbResponse;
use crate::param::Param;
use crate::Result;
use crate::Route;
use flume::Receiver;
use futures::StreamExt;
use serde_json::json;
use surrealdb::sql::Array;
use surrealdb::sql::Value;

pub(super) fn mock(route_rx: Receiver<Option<Route<(Method, Param), Result<DbResponse>>>>) {
	tokio::spawn(async move {
		let mut stream = route_rx.into_stream();

		while let Some(Some(Route {
			request,
			response,
		})) = stream.next().await
		{
			let (method, param) = request;
			let params = param.query;

			let result = match method {
				Method::Invalidate | Method::Health => match &params[..] {
					[] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Authenticate => match &params[..] {
					[_] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Kill => match &params[..] {
					[_] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Live => match &params[..] {
					[_] => Ok(DbResponse::Other(
						"c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_owned().into(),
					)),
					_ => unreachable!(),
				},
				Method::Version => match &params[..] {
					[] => Ok(DbResponse::Other("1.0.0".into())),
					_ => unreachable!(),
				},
				Method::Use => match &params[..] {
					[_] | [_, _] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Signup | Method::Signin => match &params[..] {
					[credentials] => {
						let credentials: Credentials = from_value(&credentials).unwrap();
						match credentials {
							Credentials::Root {
								..
							} => Ok(DbResponse::Other(Value::None)),
							_ => Ok(DbResponse::Other("jwt".to_owned().into())),
						}
					}
					_ => unreachable!(),
				},
				Method::Set => match &params[..] {
					[_, _] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Unset => match &params[..] {
					[_] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Query => match &params[..] {
					[_] | [_, _] => Ok(DbResponse::Query(Vec::new())),
					_ => unreachable!(),
				},
				Method::Create => match &params[..] {
					[_] => Ok(DbResponse::Other(from_json(json!(User::default())))),
					[_, user] => Ok(DbResponse::Other(user.clone())),
					_ => unreachable!(),
				},
				Method::Select => match &params[..] {
					[Value::Thing(..)] => Ok(DbResponse::Other(from_json(json!(User::default())))),
					[Value::Table(..) | Value::Array(..) | Value::Range(..)] => {
						Ok(DbResponse::Other(Value::Array(Array(Vec::new()))))
					}
					_ => unreachable!(),
				},
				Method::Update | Method::Merge | Method::Patch => match &params[..] {
					[Value::Thing(..)] | [Value::Thing(..), _] => {
						Ok(DbResponse::Other(from_json(json!(User::default()))))
					}
					[Value::Table(..) | Value::Array(..) | Value::Range(..)]
					| [Value::Table(..) | Value::Array(..) | Value::Range(..), _] => {
						Ok(DbResponse::Other(Value::Array(Array(Vec::new()))))
					}
					_ => unreachable!(),
				},
				Method::Delete => match &params[..] {
					[_] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				#[cfg(feature = "http")]
				Method::Export => match param.file {
					Some(..) => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				#[cfg(feature = "http")]
				Method::Import => match param.file {
					Some(..) => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
			};

			if let Err(message) = response.into_send_async(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
