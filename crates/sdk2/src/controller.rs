use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use std::sync::Arc;
use surrealdb_types::SurrealBridge;
use url::Url;

use crate::events::{
	Connected, Connecting, EngineConnected, EngineDisconnected, EngineReconnecting,
	Error, SurrealEvents,
};
use crate::{impl_events, subscribe_first_of};
use crate::utils::{
	ConstructableEngine, Engine, Engines, Publisher, Subscribeable,
};

/// Controller manages the connection for a Surreal client.
///
/// This layer sits between the user-facing `Surreal` API and the underlying
/// `SurrealBridge` engine. It handles:
/// - Connection lifecycle (connecting, reconnecting, disconnecting)
/// - Request routing to the bridge
///
/// Sessions and transactions are now managed at the typestate level
/// (SurrealSession, SurrealTransaction) rather than in the controller.
#[derive(Clone)]
pub struct Controller {
	publisher: Publisher<SurrealEvents>,
	engines: Engines,
	engine: Arc<ArcSwap<Option<Arc<dyn Engine>>>>,
	status: Arc<ArcSwap<ConnectionStatus>>,
}

impl_events!(Controller on publisher for SurrealEvents);

impl Controller {
	pub fn new() -> Self {
		Self {
			publisher: Publisher::new(16),
			engines: Engines::new(),
			engine: Arc::new(ArcSwap::new(Arc::new(None))),
			status: Arc::new(ArcSwap::new(Arc::new(ConnectionStatus::Disconnected))),
		}
	}

	fn set_status(&self, status: ConnectionStatus) {
		self.status.store(Arc::new(status));
	}

	pub fn attach_engine<E: ConstructableEngine>(&mut self) {
		self.engines.attach::<E>();
	}

	pub async fn connect(&self, url: &str) -> Result<()> {
		let parsed = Url::parse(url)?;
		let protocol = parsed.scheme();

		// Get the constructor for the engine
		let Some(constructor) = self.engines.constructor(protocol) else {
			bail!("No engine registered for protocol '{}'", protocol);
		};

		// Set the status to connecting
		self.set_status(ConnectionStatus::Connecting);
		self.publisher.publish(Connecting {});
		
		// Construct the engine
		let engine = constructor();

		// Subscribe to engine events - each event type needs its own subscription
		self.on_connected(engine.publisher().subscribe::<EngineConnected>());
		self.on_reconnected(engine.publisher().subscribe::<EngineReconnecting>());
		self.on_disconnected(engine.publisher().subscribe::<EngineDisconnected>());

		// Store the engine
		self.engine.store(Arc::new(Some(engine.clone())));
		// Setup is done, let the engine connect
		engine.connect(parsed);

		self.ready().await
	}

	fn on_connected(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineConnected>) {
		tokio::spawn(async move {
			while let Ok(event) = broadcast.recv().await {
				eprintln!("Engine connected: {:?}", event);
			}
		});
	}

	fn on_reconnected(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineReconnecting>) {
		tokio::spawn(async move {
			while let Ok(event) = broadcast.recv().await {
				eprintln!("Engine reconnecting: {:?}", event);
			}
		});
	}

	fn on_disconnected(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineDisconnected>) {
		tokio::spawn(async move {
			while let Ok(event) = broadcast.recv().await {
				eprintln!("Engine disconnected: {:?}", event);
			}
		});
	}

	pub fn status(&self) -> ConnectionStatus {
		self.status.load().as_ref().clone()
	}

	pub async fn ready(&self) -> Result<()> {
		match self.status.load().as_ref() {
			ConnectionStatus::Connected(_) => Ok(()),
			ConnectionStatus::Disconnected => {
				bail!("Not connected to a database. Call connect() first.");
			}
			_ => {
				subscribe_first_of!(self => {
					(connected: Connected) {
						self.set_status(ConnectionStatus::Connected(connected));
						return Ok(());
					}
					(error: Error) {
						self.set_status(ConnectionStatus::Disconnected);
						bail!("Connection failed: {}", error.message);
					}
				})
			}
		}
	}

	/// Gets the bridge, waiting if connection is in progress.
	///
	/// Returns an error if not connected or connecting.
	pub async fn bridge(&self) -> Result<Arc<dyn SurrealBridge>> {
		let engine = self.engine.load();
		
		if let Some(engine) = engine.as_ref() {
			Ok(Arc::clone(engine) as Arc<dyn SurrealBridge>)
		} else {
			bail!("Not connected to a database. Call connect() first.");
		}
	}
}

impl Subscribeable<SurrealEvents> for Controller {
	fn publisher(&self) -> &Publisher<SurrealEvents> {
		&self.publisher
	}
}

#[derive(Clone)]
pub enum ConnectionStatus {
	Disconnected,
	Connecting,
	Reconnecting,
	Connected(Connected)
}

impl ConnectionStatus {
	pub fn is_connected(&self) -> bool {
		matches!(self, ConnectionStatus::Connected(_))
	}
	pub fn is_connecting(&self) -> bool {
		matches!(self, ConnectionStatus::Connecting)
	}
	pub fn is_reconnecting(&self) -> bool {
		matches!(self, ConnectionStatus::Reconnecting)
	}
	pub fn is_disconnected(&self) -> bool {
		matches!(self, ConnectionStatus::Disconnected)
	}
}