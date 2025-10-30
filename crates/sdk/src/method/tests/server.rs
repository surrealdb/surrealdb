use async_channel::Receiver;
use surrealdb_core::dbs::QueryResultBuilder;
use surrealdb_types::SurrealValue;

use crate::conn::{Command, Route};
use crate::types::Value;

pub(super) fn mock(route_rx: Receiver<Route>) {
	tokio::spawn(async move {
		while let Ok(Route {
			request,
			response,
		}) = route_rx.recv().await
		{
			let cmd = request.command;

			let query_result = QueryResultBuilder::started_now();

			let query_result = match cmd {
				Command::Invalidate
				| Command::Health
				| Command::Revoke {
					..
				} => query_result,
				Command::Authenticate {
					token,
				}
				| Command::Refresh {
					token,
				} => query_result.with_result(Ok(token.into_value())),
				Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => query_result,
				Command::SubscribeLive {
					..
				} => query_result.with_result(Ok(Value::String(
					"c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_string(),
				))),
				Command::Version => {
					query_result.with_result(Ok(Value::String("1.0.0".to_string())))
				}
				Command::Use {
					..
				} => query_result,
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => query_result.with_result(Ok(Value::String("jwt".to_string()))),
				Command::Set {
					..
				} => query_result,
				Command::RawQuery {
					..
				} => query_result,
				Command::Run {
					..
				} => query_result,
				Command::ExportMl {
					..
				}
				| Command::ExportBytesMl {
					..
				}
				| Command::ExportFile {
					..
				}
				| Command::ExportBytes {
					..
				}
				| Command::ImportMl {
					..
				}
				| Command::ImportFile {
					..
				} => query_result,
			};

			let result = query_result.finish();

			if let Err(message) = response.send(Ok(vec![result])).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
