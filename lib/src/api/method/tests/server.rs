use super::types::User;
use crate::api::conn::Command;
use crate::api::conn::DbResponse;
use crate::api::conn::Route;
use crate::api::Response as QueryResponse;
use crate::sql::to_value;

pub(super) fn mock(route_rx: Receiver<Route>) {
	tokio::spawn(async move {
		while let Ok(Route {
			request,
			response,
		}) = route_rx.recv().await
		{
			let cmd = request.command;

			let result = match cmd {
				Command::Invalidate | Command::Health => Ok(DbResponse::Other(Value::None)),
				Command::Authenticate {
					..
				}
				| Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => Ok(DbResponse::Other(Value::None)),
				Command::SubscribeLive {
					..
				} => Ok(DbResponse::Other("c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_owned().into())),
				Command::Version => Ok(DbResponse::Other("1.0.0".into())),
				Command::Use {
					..
				} => Ok(DbResponse::Other(Value::None)),
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => Ok(DbResponse::Other("jwt".to_owned().into())),
				Command::Set {
					..
				} => Ok(DbResponse::Other(Value::None)),
				Command::Query {
					..
				} => Ok(DbResponse::Query(QueryResponse::new())),
				Command::Create {
					data,
					..
				} => match data {
					None => Ok(DbResponse::Other(to_value(User::default()).unwrap())),
					Some(user) => Ok(DbResponse::Other(user.clone())),
				},
				Command::Select {
					what,
					..
				}
				| Command::Delete {
					what,
					..
				} => match what {
					Value::Thing(..) => Ok(DbResponse::Other(to_value(User::default()).unwrap())),
					Value::Table(..) | Value::Array(..) | Value::Range(..) => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					_ => unreachable!(),
				},
				Command::Upsert {
					what,
					..
				}
				| Command::Update {
					what,
					..
				}
				| Command::Merge {
					what,
					..
				}
				| Command::Patch {
					what,
					..
				} => match what {
					Value::Thing(..) => Ok(DbResponse::Other(to_value(User::default()).unwrap())),
					Value::Table(..) | Value::Array(..) | Value::Range(..) => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					_ => unreachable!(),
				},
				Command::Insert {
					what,
					data,
				} => match (what, data) {
					(Some(Value::Table(..)), Value::Array(..)) => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					(Some(Value::Table(..)), _) => {
						Ok(DbResponse::Other(to_value(User::default()).unwrap()))
					}
					_ => unreachable!(),
				},
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
				} => Ok(DbResponse::Other(Value::None)),
			};

			if let Err(message) = response.send(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
