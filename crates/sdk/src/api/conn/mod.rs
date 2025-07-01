use crate::QueryResults;
use crate::api;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::rpc::Response;
use async_channel::Receiver;
use async_channel::Sender;
use chrono::DateTime;
use chrono::Utc;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use surrealdb_core::dbs::ResponseData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Value, from_value as from_core_value};

mod cmd;
#[cfg(feature = "protocol-http")]
pub(crate) use cmd::RouterRequest;
pub(crate) use cmd::{Command, LiveQueryParams, Request};

use super::opt::Config;


#[derive(Debug, Clone)]
pub(crate) struct MlExportConfig {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) name: String,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) version: String,
}

// /// Connection trait implemented by supported protocols
// pub trait Sealed: Sized + Send + Sync + 'static {
// 	/// Connect to the server
// 	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>>
// 	where
// 		Self: api::Connection;
// }
