use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use std::sync::Arc;
use surrealdb_types::SurrealBridge;
use url::Url;

use crate::events::{
	Connected, Connecting, Disconnected, EngineConnected, EngineDisconnected, EngineError, EngineReconnecting, Error, Reconnecting, SurrealEvents
};
use crate::{impl_events, subscribe_first_of};
use crate::utils::{
	ConnectionStatus, ConstructableEngine, Engine, Engines, Event, Publisher, Subscribeable
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

impl_events!(Controller for SurrealEvents);

impl Controller {
	pub fn new() -> Self {
		Self {
			publisher: Publisher::new(16),
			engines: Engines::new(),
			engine: Arc::new(ArcSwap::new(Arc::new(None))),
			status: Arc::new(ArcSwap::new(Arc::new(ConnectionStatus::Disconnected))),
		}
	}

	fn set_status(&self, event: impl Event<SurrealEvents> + Into<ConnectionStatus>) {
		let status = event.clone().into();
		self.status.store(Arc::new(status));
		self.publisher.publish(event);
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
		self.set_status(Connecting {});
		
		// Construct the engine
		let engine = constructor();

		// Subscribe to engine events - each event type needs its own subscription
		self.on_engine_connected(engine.publisher().subscribe::<EngineConnected>());
		self.on_engine_reconnecting(engine.publisher().subscribe::<EngineReconnecting>());
		self.on_engine_disconnected(engine.publisher().subscribe::<EngineDisconnected>());
		self.on_engine_error(engine.publisher().subscribe::<EngineError>());

		// Store the engine
		self.engine.store(Arc::new(Some(engine.clone())));
		// Setup is done, let the engine connect
		engine.connect(parsed);

		self.ready().await
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
					(_connected: Connected) {
						return Ok(());
					}
					(error: Error) {
						return Err(anyhow::anyhow!("Connection failed: {}", error.message));
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

	pub async fn version(&self) -> Result<String> {
		if let ConnectionStatus::Connected(connected) = self.status.load().as_ref() {
			return Ok(connected.version.clone());
		}

		let bridge = self.bridge().await?;
		bridge.version().await
	}


	/////////////////////////////////////////////
	/////////// Engine event handlers ///////////
	/////////////////////////////////////////////

	fn on_engine_connected(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineConnected>) {
		let this = self.clone();
		tokio::spawn(async move {
			while broadcast.recv().await.is_ok() {
				match this.version().await {
					Ok(version) => {
						this.set_status(Connected { version });
					}
					Err(e) => {
						this.set_status(Error { 
							message: format!("Failed to get version: {}", e)
						});
					}
				}
			}
		});
	}

	fn on_engine_reconnecting(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineReconnecting>) {
		let this = self.clone();
		tokio::spawn(async move {
			while broadcast.recv().await.is_ok() {
				this.set_status(Reconnecting {});
			}
		});
	}

	fn on_engine_disconnected(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineDisconnected>) {
		let this = self.clone();
		tokio::spawn(async move {
			while broadcast.recv().await.is_ok() {
				this.set_status(Disconnected {});
			}
		});
	}

	fn on_engine_error(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineError>) {
		let this = self.clone();
		tokio::spawn(async move {
			while let Ok(error) = broadcast.recv().await {
				this.set_status(Error { message: format!("Engine error: {}", error.message) });
			}
		});
	}
}

impl Subscribeable<SurrealEvents> for Controller {
	fn publisher(&self) -> &Publisher<SurrealEvents> {
		&self.publisher
	}
}