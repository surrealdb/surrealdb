use crate::Action;
use crate::Surreal;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::method::Live;
use crate::method::Query;
use crate::method::Select;
use crate::opt::Resource;
use crate::opt::SubscribableResource;
use crate::value::Notification;
use async_channel::Receiver;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use surrealdb_core::dbs::{Action as CoreAction, Notification as CoreNotification};
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{
	Cond, Expression, Field, Fields, Ident, Idiom, Operator, Part, Table, Thing, Value,
	statements::LiveStatement,
};
use uuid::Uuid;

#[cfg(not(target_family = "wasm"))]
use tokio::spawn;

#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

const ID: &str = "id";

fn deserialize<R>(notification: CoreNotification) -> Option<Result<crate::Notification<R>>>
where
	R: TryFromValue,
{
	let query_id = *notification.id;
	let action = notification.action;
	match action {
		CoreAction::Killed => None,
		action => match R::try_from_value(notification.result) {
			Ok(data) => Some(Ok(Notification {
				query_id,
				data,
				action: Action::from_core(action),
			})),
			Err(error) => Some(Err(error)),
		},
	}
}
