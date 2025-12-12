use anyhow::{Result, bail};
use arc_swap::{ArcSwap, Guard};
use bytes::Bytes;
use futures::Stream;
use uuid::Uuid;
use std::sync::Arc;
use surrealdb_types::{ExportConfig, Nullable, QueryChunk, SurrealBridge, Tokens, Value, Variables};
use url::Url;

use crate::api::SurrealSession;
use crate::events::{
	Connected, Connecting, Disconnected, EngineConnected, EngineDisconnected, EngineError, EngineReconnecting, Error, Reconnecting, SurrealEvents, Using
};
use crate::{impl_events, subscribe_first_of};
use crate::utils::{
	ConnectionState, ConnectionStatus, ConstructableEngine, Engine, Engines, Event, Publisher, SessionState, Subscribeable
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
	state: Arc<ArcSwap<ConnectionState>>,
}

impl_events!(Controller for SurrealEvents);

impl Controller {
	pub fn new() -> Self {
		Self {
			publisher: Publisher::new(16),
			engines: Engines::new(),
			engine: Arc::new(ArcSwap::new(Arc::new(None))),
			status: Arc::new(ArcSwap::new(Arc::new(ConnectionStatus::Disconnected))),
			state: Arc::new(ArcSwap::new(Arc::new(ConnectionState::default()))),
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

		// Update the state
		let state = ConnectionState::new(url.to_string(), SessionState::default());
		self.state.store(Arc::new(state));

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
		engine.connect(self.state.load_full());

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
	fn bridge(&self) -> Result<Arc<dyn SurrealBridge>> {
		let engine = self.engine.load();
		
		if let Some(engine) = engine.as_ref() {
			Ok(Arc::clone(engine) as Arc<dyn SurrealBridge>)
		} else {
			bail!("Not connected to a database. Call connect() first.");
		}
	}

	fn state(&self) -> Guard<Arc<ConnectionState>> {
		self.state.load()
	}

	pub fn new_session(&self) -> SurrealSession {
		let uuid = self.state().new_session();
		SurrealSession::new(self.clone(), uuid)
	}

	pub async fn health(&self) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.health().await
	}

	pub async fn version(&self) -> Result<String> {
		// We cache the version upon connection
		if let ConnectionStatus::Connected(connected) = self.status.load().as_ref() {
			return Ok(connected.version.clone());
		}

		let bridge = self.bridge()?;
		bridge.version().await
	}

	pub async fn drop_session(&self, session_id: Uuid) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.drop_session(session_id).await?;
		self.state().remove_session(session_id);
		Ok(())
	}

	pub async fn reset_session(&self, session_id: Option<Uuid>) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.reset_session(session_id).await?;
		self.state().upsert_session(session_id, SessionState::default()).await;
		Ok(())
	}

	pub async fn list_sessions(&self) -> Result<Vec<Uuid>> {
		let bridge = self.bridge()?;
		bridge.list_sessions().await
	}

	// Session modifiers
	pub async fn r#use(&self, session_id: Option<Uuid>, ns: Nullable<String>, db: Nullable<String>) -> Result<(Option<String>, Option<String>)> {
		// Requirements
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());
		
		// Execute the request
		let (ns, db) = bridge.r#use(session_id.clone(), ns, db).await?;

		// Update the session state
		let mut session = state.write().await;
		session.namespace = ns.clone();
		session.database = db.clone();

		// Publish the event
		self.publisher.publish(Using { 
			session_id,
			namespace: ns.clone(), 
			database: db.clone() 
		});

		// Ok
		Ok((ns, db))
	}

	pub async fn set_variable(&self, session_id: Option<Uuid>, name: String, value: Value) -> Result<()> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		bridge.set_variable(session_id, name.clone(), value.clone()).await?;

		// Update the session state
		let mut session = state.write().await;
		session.variables.insert(name, value);

		// Ok
		Ok(())
	}

	pub async fn drop_variable(&self, session_id: Option<Uuid>, name: String) -> Result<()> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		bridge.drop_variable(session_id, name.clone()).await?;

		// Update the session state
		let mut session = state.write().await;
		session.variables.remove(&name);

		// Ok
		Ok(())
	}

	// Transactions
	pub async fn begin_transaction(&self, session_id: Option<Uuid>) -> Result<Uuid> {
		let bridge = self.bridge()?;
		bridge.begin_transaction(session_id).await
	}

	pub async fn commit_transaction(&self, session_id: Option<Uuid>, transaction_id: Uuid) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.commit_transaction(session_id, transaction_id).await
	}

	pub async fn cancel_transaction(&self, session_id: Option<Uuid>, transaction_id: Uuid) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.cancel_transaction(session_id, transaction_id).await
	}

	pub async fn list_transactions(&self, session_id: Option<Uuid>) -> Result<Vec<Uuid>> {
		let bridge = self.bridge()?;
		bridge.list_transactions(session_id).await
	}

	// Authentication
	pub async fn signup(&self, session_id: Option<Uuid>, params: Variables) -> Result<Tokens> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		let tokens = bridge.signup(session_id, params).await?;

		// Update the session state
		let mut session = state.write().await;
		if let Some(refresh) = &tokens.refresh {
			session.refresh_token = Some(refresh.clone());
		}
		if let Some(access) = &tokens.access {
			session.access_token = Some(access.clone());
		}

		// Ok
		Ok(tokens)
	}

	pub async fn signin(&self, session_id: Option<Uuid>, params: Variables) -> Result<Tokens> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		let tokens = bridge.signin(session_id, params).await?;

		// Update the session state
		let mut session = state.write().await;
		if let Some(access) = &tokens.access {
			session.access_token = Some(access.clone());
		}
		if let Some(refresh) = &tokens.refresh {
			session.refresh_token = Some(refresh.clone());
		}

		// Ok
		Ok(tokens)
	}

	pub async fn authenticate(&self, session_id: Option<Uuid>, token: String) -> Result<()> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		bridge.authenticate(session_id, token.clone()).await?;

		// Update the session state
		let mut session = state.write().await;
		session.access_token = Some(token);

		// Ok
		Ok(())
	}

	pub async fn refresh(&self, session_id: Option<Uuid>, tokens: Tokens) -> Result<Tokens> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		let tokens = bridge.refresh(session_id, tokens).await?;

		// Update the session state
		let mut session = state.write().await;
		if let Some(access) = &tokens.access {
			session.access_token = Some(access.clone());
		}
		if let Some(refresh) = &tokens.refresh {
			session.refresh_token = Some(refresh.clone());
		}

		// Ok
		Ok(tokens)
	}

	pub async fn revoke(&self, tokens: Tokens) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.revoke(tokens).await
	}

	pub async fn invalidate(&self, session_id: Option<Uuid>) -> Result<()> {
		let bridge = self.bridge()?;
		let state = self.state().get_session(session_id.clone());

		// Execute the request
		bridge.invalidate(session_id).await?;

		// Update the session state
		let mut session = state.write().await;
		session.access_token = None;
		session.refresh_token = None;

		// Ok
		Ok(())
	}

	// Export & Import
	pub async fn export(&self, session_id: Option<Uuid>, config: ExportConfig) -> Result<std::pin::Pin<Box<dyn Stream<Item = Bytes> + Send>>> {
		let bridge = self.bridge()?;
		bridge.export(session_id, config).await
	}

	pub async fn import(&self, session_id: Option<Uuid>, sql: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>) -> Result<()> {
		let bridge = self.bridge()?;
		bridge.import(session_id, sql).await
	}

	// Query
	pub async fn query(&self, session_id: Option<Uuid>, txn: Option<Uuid>, query: String, vars: Variables) -> Result<std::pin::Pin<Box<dyn Stream<Item = QueryChunk> + Send>>> {
		let bridge = self.bridge()?;
		bridge.query(session_id, txn, query, vars).await
	}
}


/////////////////////////////////////////////
//////////// Engine event handlers ///////////
/////////////////////////////////////////////

impl Controller {
	fn on_engine_connected(&self, mut broadcast: tokio::sync::broadcast::Receiver<EngineConnected>) {
		let this = self.clone();
		tokio::spawn(async move {
			while broadcast.recv().await.is_ok() {
				if let Err(e) = this.prepare_connection().await {
					this.set_status(Error { message: format!("Failed to prepare connection: {}", e) });
				}
			}
		});
	}

	async fn prepare_connection(&self) -> Result<()> {
		let bridge = self.bridge()?;
		let version = bridge.version().await?;

		for session in self.state().all_sessions() {
			let session = session.read().await;

			// Restore the namespace and database
			if session.namespace.is_some() {
				bridge.r#use(
					session.id, 
					session.namespace.clone().into(), 
					session.database.clone().into()
				).await?;
			}

			// Restore the variables
			for (name, value) in session.variables.iter() {
				bridge.set_variable(session.id, name.clone(), value.clone()).await?;
			}

			// TODO restore auth
		}

		self.set_status(Connected { version });
		Ok(())
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