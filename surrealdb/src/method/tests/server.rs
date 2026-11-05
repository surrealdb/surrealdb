use std::sync::Mutex;

use async_channel::Receiver;
use surrealdb_rpc::QueryResultBuilder;

use crate::conn::{Command, Route};
use crate::types::{SurrealValue, Value};

/// The transaction ids the mock has been asked to roll back, in order.
pub(super) static ROLLED_BACK: Mutex<Vec<uuid::Uuid>> = Mutex::new(Vec::new());

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
				Command::Invalidate | Command::Health => query_result,
				Command::Begin {
					..
				} => query_result.with_result(Ok(Value::Uuid(uuid::Uuid::now_v7().into()))),
				Command::Rollback {
					txn,
				} => {
					ROLLED_BACK.lock().expect("rollback log").push(txn);
					query_result
				}
				Command::Commit {
					..
				}
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
				Command::Query {
					..
				} => query_result,
				Command::Run {
					..
				} => query_result,
				Command::ExportFile {
					..
				}
				| Command::ExportBytes {
					..
				}
				| Command::ImportFile {
					..
				}
				| Command::Attach {
					..
				}
				| Command::Detach {
					..
				} => query_result,
			};

			let result = query_result.finish();

			// A caller that cancelled its request has dropped the receiver.
			response.send(Ok(vec![result])).await.ok();
		}
	});
}
