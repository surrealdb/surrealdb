//! WebSocket engine

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

use crate::api::Result;
use crate::api::Surreal;
use crate::api::conn::Command;
use crate::dbs::ResponseData;
use crate::opt::IntoEndpoint;
use async_channel::Sender;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use surrealdb_core::dbs::Notification;
use surrealdb_core::expr::Value;
use trice::Instant;
use uuid::Uuid;

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const REVISION_HEADER: &str = "revision";

/// The WS scheme used to connect to `grpc://` endpoints
#[derive(Debug)]
pub struct Grpc;

/// The WSS scheme used to connect to `grpcs://` endpoints
#[derive(Debug)]
pub struct Grpcs;
