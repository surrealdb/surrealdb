use super::types::Credentials;
use super::types::User;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Route;
use crate::api::opt::from_json;
use crate::api::opt::from_value;
use crate::api::Response as QueryResponse;
use crate::sql::Array;
use crate::sql::Value;
use flume::Receiver;
use futures::StreamExt;
use serde_json::json;
use std::mem;

pub(super) fn mock(route_rx: Receiver<Option<Route>>) {
	tokio::spawn(async move {
		let mut stream = route_rx.into_stream();

		while let Some(Some(Route {
			request,
			response,
		})) = stream.next().await
		{
			let (_, method, param) = request;
			let mut params = param.other;

			let result = match method {
				Method::Invalidate | Method::Health => match &params[..] {
					[] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Authenticate | Method::Kill | Method::Unset | Method::Delete => {
					match &params[..] {
						[_] => Ok(DbResponse::Other(Value::None)),
						_ => unreachable!(),
					}
				}
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
				Method::Signup | Method::Signin => match &mut params[..] {
					[credentials] => {
						let credentials: Credentials = from_value(mem::take(credentials)).unwrap();
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
				Method::Query => match param.query {
					Some(_) => Ok(DbResponse::Query(QueryResponse(Default::default()))),
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
				Method::Export | Method::Import => match param.file {
					Some(_) => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
			};

			if let Err(message) = response.into_send_async(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
