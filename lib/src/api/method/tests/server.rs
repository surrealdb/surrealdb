use super::types::User;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Route;
use crate::api::Response as QueryResponse;
use crate::sql::to_value;
use crate::Value;
use flume::Receiver;
use futures::StreamExt;

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
				Method::Authenticate | Method::Kill | Method::Unset => match &params[..] {
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
				Method::Signup | Method::Signin => match &mut params[..] {
					[_] => Ok(DbResponse::Other("jwt".to_owned().into())),
					_ => unreachable!(),
				},
				Method::Set => match &params[..] {
					[_, _] => Ok(DbResponse::Other(Value::None)),
					_ => unreachable!(),
				},
				Method::Query => match param.query {
					Some(_) => Ok(DbResponse::Query(QueryResponse::new())),
					_ => unreachable!(),
				},
				Method::Create => match &params[..] {
					[_] => Ok(DbResponse::Other(to_value(User::default()).unwrap())),
					[_, user] => Ok(DbResponse::Other(user.clone())),
					_ => unreachable!(),
				},
				Method::Select | Method::Delete => match &params[..] {
					[Value::Thing(..)] => Ok(DbResponse::Other(to_value(User::default()).unwrap())),
					[Value::Table(..) | Value::Array(..) | Value::Range(..)] => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					_ => unreachable!(),
				},
				Method::Upsert | Method::Update | Method::Merge | Method::Patch => {
					match &params[..] {
						[Value::Thing(..)] | [Value::Thing(..), _] => {
							Ok(DbResponse::Other(to_value(User::default()).unwrap()))
						}
						[Value::Table(..) | Value::Array(..) | Value::Range(..)]
						| [Value::Table(..) | Value::Array(..) | Value::Range(..), _] => {
							Ok(DbResponse::Other(Value::Array(Default::default())))
						}
						_ => unreachable!(),
					}
				}
				Method::Insert => match &params[..] {
					[Value::Table(..), Value::Array(..)] => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					[Value::Table(..), _] => {
						Ok(DbResponse::Other(to_value(User::default()).unwrap()))
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
