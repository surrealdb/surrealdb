//! WebSocket engine

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use async_channel::Sender;
use indexmap::IndexMap;
use trice::Instant;
use uuid::Uuid;

use crate::api::conn::{Command, DbResponse};
use crate::api::{Connect, Result, Surreal};
use crate::core::dbs::Notification;
use crate::core::val::Value as CoreValue;
use crate::opt::IntoEndpoint;

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const REVISION_HEADER: &str = "revision";

enum RequestEffect {
	/// Completing this request sets a variable to a give value.
	Set {
		key: String,
		value: CoreValue,
	},
	/// Completing this request sets a variable to a give value.
	Clear {
		key: String,
	},
	/// Insert requests repsonses need to be flattened in an array.
	Insert,
	/// No effect
	None,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
enum ReplayMethod {
	Use,
	Signup,
	Signin,
	Invalidate,
	Authenticate,
}

struct PendingRequest {
	// Does resolving this request has some effects.
	effect: RequestEffect,
	// The channel to send the result of the request into.
	response_channel: Sender<Result<DbResponse>>,
}

struct RouterState<Sink, Stream> {
	/// Vars currently set by the set method,
	vars: IndexMap<String, CoreValue>,
	/// Messages which aught to be replayed on a reconnect.
	replay: IndexMap<ReplayMethod, Command>,
	/// Pending live queries
	live_queries: HashMap<Uuid, async_channel::Sender<Notification>>,
	/// Send requests which are still awaiting an awnser.
	pending_requests: HashMap<i64, PendingRequest>,
	/// The last time a message was recieved from the server.
	last_activity: Instant,
	/// The sink into which messages are send to surrealdb
	sink: Sink,
	/// The stream from which messages are recieved from surrealdb
	stream: Stream,
}

impl<Sink, Stream> RouterState<Sink, Stream> {
	pub fn new(sink: Sink, stream: Stream) -> Self {
		RouterState {
			vars: IndexMap::new(),
			replay: IndexMap::new(),
			live_queries: HashMap::new(),
			pending_requests: HashMap::new(),
			last_activity: Instant::now(),
			sink,
			stream,
		}
	}
}

enum HandleResult {
	/// Socket disconnected, should continue to reconnect
	Disconnected,
	/// Nothing wrong continue as normal.
	Ok,
}

/// The WS scheme used to connect to `ws://` endpoints
#[derive(Debug)]
pub struct Ws;

/// The WSS scheme used to connect to `wss://` endpoints
#[derive(Debug)]
pub struct Wss;

/// A WebSocket client for communicating with the server via WebSockets
#[derive(Debug, Clone)]
pub struct Client(());

impl Surreal<Client> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::ws::Client;
	/// use surrealdb::engine::remote::ws::Ws;
	///
	/// static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect::<Ws>("localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			surreal: self.inner.clone().into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}
