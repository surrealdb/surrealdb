//! WebSocket engine

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

use std::time::Duration;

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const REVISION_HEADER: &str = "revision";

/// The WS scheme used to connect to `grpc://` endpoints
#[derive(Debug)]
pub struct Grpc;

/// The WSS scheme used to connect to `grpcs://` endpoints
#[derive(Debug)]
pub struct Grpcs;
