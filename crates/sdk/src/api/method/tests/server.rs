use async_channel::Receiver;

use super::types::User;
use crate::api::conn::{Command, DbResponse, Route};
use crate::api::{self, Response as QueryResponse};
use crate::core::val;
use crate::opt::Resource;

pub(super) fn mock(route_rx: Receiver<Route>) {
	tokio::spawn(async move {
		while let Ok(Route {
			request,
			response,
		}) = route_rx.recv().await
		{
			let cmd = request.command;

			let result = match cmd {
				Command::Invalidate | Command::Health => Ok(DbResponse::Other(val::Value::None)),
				Command::Authenticate {
					..
				}
				| Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => Ok(DbResponse::Other(val::Value::None)),
				Command::SubscribeLive {
					..
				} => Ok(DbResponse::Other("c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_owned().into())),
				Command::Version => Ok(DbResponse::Other("1.0.0".into())),
				Command::Use {
					..
				} => Ok(DbResponse::Other(val::Value::None)),
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => Ok(DbResponse::Other("jwt".to_owned().into())),
				Command::Set {
					..
				} => Ok(DbResponse::Other(val::Value::None)),
				Command::Query {
					..
				}
				| Command::RawQuery {
					..
				}
				| Command::Patch {
					..
				}
				| Command::Merge {
					..
				} => Ok(DbResponse::Query(QueryResponse::new())),
				Command::Create {
					data,
					..
				} => match data {
					None => {
						Ok(DbResponse::Other(api::value::to_core_value(User::default()).unwrap()))
					}
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
					Resource::Table(..) | Resource::Array(..) | Resource::Range(_) => {
						Ok(DbResponse::Other(val::Value::Array(Default::default())))
					}
					Resource::RecordId(..) => {
						Ok(DbResponse::Other(api::value::to_core_value(User::default()).unwrap()))
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
				} => match what {
					Resource::Table(..) | Resource::Array(..) | Resource::Range(..) => {
						Ok(DbResponse::Other(val::Value::Array(Default::default())))
					}
					Resource::RecordId(..) => {
						Ok(DbResponse::Other(api::value::to_core_value(User::default()).unwrap()))
					}
					_ => unreachable!(),
				},
				Command::Insert {
					data,
					..
				} => match data {
					val::Value::Array(..) => {
						Ok(DbResponse::Other(val::Value::Array(Default::default())))
					}
					_ => Ok(DbResponse::Other(api::value::to_core_value(User::default()).unwrap())),
				},
				Command::InsertRelation {
					data,
					..
				} => match data {
					val::Value::Array(..) => {
						Ok(DbResponse::Other(val::Value::Array(Default::default())))
					}
					_ => Ok(DbResponse::Other(api::value::to_core_value(User::default()).unwrap())),
				},
				Command::Run {
					..
				} => Ok(DbResponse::Other(val::Value::None)),
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
				} => Ok(DbResponse::Other(val::Value::None)),
			};

			if let Err(message) = response.send(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
