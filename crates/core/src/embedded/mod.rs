use std::sync::Arc;
use arc_swap::ArcSwap;
use bytes::Bytes;
use dashmap::{mapref::one::{Ref, RefMut}, DashMap};
use anyhow::{bail, Result};
use futures::{Stream, StreamExt};
use sdk2::{events::{EngineConnected, EngineDisconnected, EngineError, EngineEvents}, utils::{ConnectionState, ConstructableEngine, Engine, Publisher}};
use surrealdb_types::{Nullable, QueryChunk, QueryResponseKind, QueryStats, QueryType, SurrealBridge};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
use crate::{dbs::Session, iam::Token, kvs::{Datastore, LockType, Transaction, TransactionType}};
use crate::types::{PublicDuration, PublicNotification, PublicTokens, PublicValue, PublicVariables};

#[derive(Clone)]
pub struct EmbeddedSurrealEngine {
    publisher: Publisher<EngineEvents>,
    datastore: Arc<ArcSwap<Option<Arc<Datastore>>>>,
    sessions: LowMap<Session>,
    transactions: LorMap<Transaction>,
}

impl EmbeddedSurrealEngine {
    fn new() -> Self {
        Self {
            publisher: Publisher::new(16),
            datastore: Arc::new(ArcSwap::new(Arc::new(None))),
            sessions: LowMap::new(),
            transactions: LorMap::new(),
        }
    }

    fn ds(&self) -> Result<Arc<Datastore>> {
        match self.datastore.load().as_ref() {
            Some(ds) => Ok(Arc::clone(ds)),
            None => bail!("No datastore initialized"),
        }
    }

    /// Gets or creates the default session (UUID nil).
    async fn default_session(&self) -> Result<Uuid> {
        let default_id = Uuid::nil();
        if self.sessions.get(&default_id).is_none() {
            self.sessions.insert(default_id, Arc::new(Session::default())).await?;
        }
        Ok(default_id)
    }

    /// Resolves an optional session ID to a concrete ID (using default if None).
    async fn resolve_session(&self, session_id: Option<Uuid>) -> Result<Uuid> {
        match session_id {
            Some(id) => Ok(id),
            None => self.default_session().await,
        }
    }
}

impl ConstructableEngine for EmbeddedSurrealEngine {
    fn protocols() -> &'static [&'static str] {
        &["memory", "surrealkv"]
    }

    fn construct() -> Self {
        EmbeddedSurrealEngine::new()
    }
}

impl Engine for EmbeddedSurrealEngine {
    fn publisher(&self) -> &Publisher<EngineEvents> {
        &self.publisher
    }

    fn connect(&self, state: Arc<ConnectionState>) {
        let this = self.clone();
        tokio::spawn(async move {
            match Datastore::new(state.url.as_ref()).await {
                Ok(ds) => {
                    this.datastore.store(Arc::new(Some(Arc::new(ds))));
                    this.publisher.publish(EngineConnected {});
                }
                Err(e) => {
                    this.publisher.publish(EngineError { message: format!("Failed to connect to datastore: {}", e) });
                }
            }
        });
    }

    fn disconnect(&self) {
        self.datastore.store(Arc::new(None));
        // self.sessions.clear();
        // self.transactions.clear();
        self.publisher.publish(EngineDisconnected {});
    }
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl SurrealBridge for EmbeddedSurrealEngine {
    async fn health(&self) -> Result<()> {
        self.ds()?.health_check().await
    }

    async fn version(&self) -> Result<String> {
        // TODO: datastore doesnt expose version. Should we include this in the build process?
        Ok("0.0.0".to_string())
    }

    async fn drop_session(&self, session_id: Uuid) -> Result<()> {
        self.sessions.remove(&session_id).await?;
        Ok(())
    }

    async fn reset_session(&self, session_id: Option<Uuid>) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();

        crate::iam::reset::reset(&mut new);
        *session = Arc::new(new);

        Ok(())
    }

    async fn list_sessions(&self) -> Result<Vec<Uuid>> {
        Ok(self.sessions.keys().collect())
    }
    
    async fn r#use(&self, session_id: Option<Uuid>, ns: Nullable<String>, db: Nullable<String>) -> Result<(Option<String>, Option<String>)> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            return Ok((None, None));
        };

        let mut new = session.as_ref().clone();

        // Convert Nullable to Option for storage
        // Null and None both map to None for session storage
        let ns_opt = match ns {
            Nullable::Some(v) => Some(v),
            Nullable::Null | Nullable::None => None,
        };
        let db_opt = match db {
            Nullable::Some(v) => Some(v),
            Nullable::Null | Nullable::None => None,
        };

        new.ns = ns_opt.clone();
        new.db = db_opt.clone();

        if new.ns.is_none() && new.db.is_some() {
            new.db = None;
        }

        let result_ns = new.ns.clone();
        let result_db = new.db.clone();

        *session = Arc::new(new);

        Ok((result_ns, result_db))
    }

    async fn set_variable(&self, session_id: Option<Uuid>, name: String, value: PublicValue) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();
        new.variables.insert(name, value);
        *session = Arc::new(new);
        Ok(())
    }

    async fn drop_variable(&self, session_id: Option<Uuid>, name: String) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();
        new.variables.remove(&name);
        *session = Arc::new(new);
        Ok(())
    }

    async fn begin_transaction(&self, session_id: Option<Uuid>) -> Result<Uuid> {
        let session_id = self.resolve_session(session_id).await?;
        if self.sessions.get(&session_id).is_none() {
        // TODO transactions should live under sessions probably
            bail!("Session not found");
        };

        let tx = self.ds()?.transaction(TransactionType::Write, LockType::Optimistic).await?;
        let id = Uuid::now_v7();
        self.transactions.insert(id, Arc::new(tx)).await?;
        Ok(id)
    }

    async fn commit_transaction(&self, session_id: Option<Uuid>, transaction_id: Uuid) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        if self.sessions.get(&session_id).is_none() {
            // TODO transactions should live under sessions probably
            bail!("Session not found");
        };

        let Some(tx) = self.transactions.remove(&transaction_id).await? else {
            bail!("Transaction not found");
        };

        tx.commit().await?;
        Ok(())
    }
    
    async fn cancel_transaction(&self, session_id: Option<Uuid>, transaction_id: Uuid) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        if self.sessions.get(&session_id).is_none() {
            // TODO transactions should live under sessions probably
            bail!("Session not found");
        };

        let Some(tx) = self.transactions.remove(&transaction_id).await? else {
            bail!("Transaction not found");
        };

        tx.cancel().await?;
        Ok(())
    }

    async fn list_transactions(&self, session_id: Option<Uuid>) -> Result<Vec<Uuid>> {
        let session_id = self.resolve_session(session_id).await?;
        if self.sessions.get(&session_id).is_none() {
            bail!("Session not found");
        };

        Ok(self.transactions.keys().collect())
    }

    async fn signup(&self, session_id: Option<Uuid>, params: PublicVariables) -> Result<PublicTokens> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();

        let ds = self.ds()?;
        let out = crate::iam::signup::signup(&ds, &mut new, params).await?;
        *session = Arc::new(new);

        Ok(out.into())
    }

    async fn signin(&self, session_id: Option<Uuid>, params: PublicVariables) -> Result<PublicTokens> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();

        let ds = self.ds()?;
        let out = crate::iam::signin::signin(&ds, &mut new, params).await?;
        *session = Arc::new(new);

        Ok(out.into())
    }

    async fn authenticate(&self, session_id: Option<Uuid>, token: String) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();

        let ds = self.ds()?;
        crate::iam::verify::token(&ds, &mut new, &token).await?;
        *session = Arc::new(new);

        Ok(())
    }

    async fn refresh(&self, session_id: Option<Uuid>, tokens: PublicTokens) -> Result<PublicTokens> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();
        let token: Token = tokens.try_into()?;
        let ds = self.ds()?;
        let out = token.refresh(&ds, &mut new).await?;
        *session = Arc::new(new);

        Ok(out.into())
    }

    async fn revoke(&self, tokens: PublicTokens) -> Result<()> {
        let token: Token = tokens.try_into()?;
        let ds = self.ds()?;
        token.revoke_refresh_token(&ds).await?;
        Ok(())
    }

    async fn invalidate(&self, session_id: Option<Uuid>) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(mut session) = self.sessions.get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();
        crate::iam::clear::clear(&mut new)?;
        *session = Arc::new(new);

        Ok(())
    }

    async fn export(&self, session_id: Option<Uuid>, config: crate::types::ExportConfig) -> Result<std::pin::Pin<Box<dyn Stream<Item = Bytes> + Send>>> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(session) = self.sessions.get(&session_id) else {
            bail!("Session not found");
        };

        let (chn, rcv) = crate::channel::bounded(1);
        self.ds()?.export_with_config(session.as_ref(), chn, config).await?.await?;

        Ok(Box::pin(rcv.map(Bytes::from)))
    }

    async fn import(&self, session_id: Option<Uuid>, sql: std::pin::Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send>>) -> Result<()> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(session) = self.sessions.get(&session_id) else {
            bail!("Session not found");
        };

        self.ds()?.import_stream(session.as_ref(), sql).await?;
        Ok(())
    }

    async fn query(&self, session_id: Option<Uuid>, txn: Option<Uuid>, query: String, vars: PublicVariables) -> Result<std::pin::Pin<Box<dyn Stream<Item = QueryChunk> + Send>>> {
        let session_id = self.resolve_session(session_id).await?;
        let Some(session) = self.sessions.get(&session_id) else {
            bail!("Session not found");
        };

        let res = if let Some(txn_id) = txn {
            let Some(tx) = self.transactions.get(&txn_id).await? else {
                return Err(anyhow::anyhow!("Transaction not found"));
            };

            self.ds()?.execute_with_transaction(&query, session.as_ref(), Some(vars), tx.clone())
                .await?
        } else {
            self.ds()?.execute(&query, &session, Some(vars)).await?
        };

        let chunks = res.into_iter().enumerate().map(|(idx, query_result)| {
            let (result, error) = match query_result.result {
                Ok(value) => {
                    // Value from QueryResult is already PublicValue (surrealdb_types::Value)
                    let public_values = match value {
                        crate::types::PublicValue::Array(arr) => arr.into_vec(),
                        v => vec![v],
                    };
                    (Some(public_values), None)
                },
                Err(e) => (None, Some(e.to_string())),
            };

            let query_type = match query_result.query_type {
                crate::dbs::QueryType::Live => Some(QueryType::Live),
                crate::dbs::QueryType::Kill => Some(QueryType::Kill),
                crate::dbs::QueryType::Other => Some(QueryType::Other),
            };

            QueryChunk {
                query: idx as u64,
                batch: 0,
                kind: QueryResponseKind::Single,
                stats: Some(QueryStats {
                    records_received: 0,
                    bytes_received: 0,
                    records_scanned: 0,
                    bytes_scanned: 0,
                    duration: PublicDuration::from(query_result.time),
                }),
                result,
                r#type: query_type,
                error,
            }
        });

        Ok(Box::pin(futures::stream::iter(chunks)))
    }

    async fn notifications(&self) -> Result<std::pin::Pin<Box<dyn Stream<Item = PublicNotification> + Send>>> {
        let Some(stream) = self.ds()?.notifications() else {
            bail!("Notifications not enabled");
        };

        Ok(Box::pin(stream.map(PublicNotification::from)))
    }
}

// Lock on write map
// Reads are free
pub struct LowMap<T>(DashMap<uuid::Uuid, (Arc<Semaphore>, Arc<T>)>);

impl<T> Clone for LowMap<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> LowMap<T> {
	pub fn new() -> Self {
		Self(DashMap::new())
	}

	pub fn get(&self, id: &uuid::Uuid) -> Option<Arc<T>> {
		self.0.get(id).map(|entry| entry.value().1.clone())
	}

	pub async fn get_mut(&self, id: &uuid::Uuid) -> anyhow::Result<Option<LowMutGuard<'_, T>>> {
		let Some(value) = self.0.get_mut(id) else {
			return Ok(None);
		};

        let permit = value.0.clone().acquire_owned().await?;

        Ok(Some(LowMutGuard::new(value, permit)))
	}

	pub async fn insert(&self, id: uuid::Uuid, value: Arc<T>) -> anyhow::Result<Option<Arc<T>>> {
		// If entry exists, acquire its lock first to ensure exclusive access
		let _permit = if let Some(entry) = self.0.get(&id) {
			Some(entry.value().0.clone().acquire_owned().await?)
		} else {
			None
		};

		let lock = Arc::new(Semaphore::new(1));
		Ok(self.0.insert(id, (lock, value)).map(|entry| entry.1.clone()))
	}

	pub async fn remove(&self, id: &uuid::Uuid) -> anyhow::Result<Option<Arc<T>>> {
		// Acquire the lock first to ensure exclusive access
		let _permit = if let Some(entry) = self.0.get(id) {
			Some(entry.value().0.clone().acquire_owned().await?)
		} else {
			None
		};

		Ok(self.0.remove(id).map(|entry| entry.1.1.clone()))
	}

	pub fn keys(&self) -> impl Iterator<Item = uuid::Uuid> {
		self.0.iter().map(|entry| entry.key().clone())
	}
}
pub struct LowMutGuard<'a, T> {
	value: RefMut<'a, uuid::Uuid, (Arc<Semaphore>, Arc<T>)>,
	_permit: OwnedSemaphorePermit,
}

impl<'a, T> LowMutGuard<'a, T> {
	pub fn new(value: RefMut<'a, uuid::Uuid, (Arc<Semaphore>, Arc<T>)>, permit: OwnedSemaphorePermit) -> Self {
		Self { value, _permit: permit }
	}
}

impl<'a, T> std::ops::Deref for LowMutGuard<'a, T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Self::Target {
        &self.value.1
    }
}

impl<'a, T> std::ops::DerefMut for LowMutGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut (*self.value).1
    }
}

// Lock on read map
pub struct LorMap<T>(DashMap<uuid::Uuid, (Arc<Semaphore>, Arc<T>)>);

impl<T> Clone for LorMap<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> LorMap<T> {
	pub fn new() -> Self {
		Self(DashMap::new())
	}

	pub async fn get(&self, id: &uuid::Uuid) -> anyhow::Result<Option<LorGuard<'_, T>>> {
		let Some(value) = self.0.get(id) else {
			return Ok(None);
		};

        let permit = value.0.clone().acquire_owned().await?;

        Ok(Some(LorGuard::new(value, permit)))
	}

	pub async fn get_mut(&self, id: &uuid::Uuid) -> anyhow::Result<Option<LorMutGuard<'_, T>>> {
		let Some(value) = self.0.get_mut(id) else {
			return Ok(None);
		};

        let permit = value.0.clone().acquire_owned().await?;

        Ok(Some(LorMutGuard::new(value, permit)))
	}

	pub async fn insert(&self, id: uuid::Uuid, value: Arc<T>) -> anyhow::Result<Option<Arc<T>>> {
		// If entry exists, acquire its lock first to ensure exclusive access
		let _permit = if let Some(entry) = self.0.get(&id) {
			Some(entry.value().0.clone().acquire_owned().await?)
		} else {
			None
		};

		let lock = Arc::new(Semaphore::new(1));
		Ok(self.0.insert(id, (lock, value)).map(|entry| entry.1.clone()))
	}

	pub async fn remove(&self, id: &uuid::Uuid) -> anyhow::Result<Option<Arc<T>>> {
		// Acquire the lock first to ensure exclusive access
		let _permit = if let Some(entry) = self.0.get(id) {
			Some(entry.value().0.clone().acquire_owned().await?)
		} else {
			None
		};

		Ok(self.0.remove(id).map(|entry| entry.1.1.clone()))
	}

	pub fn keys(&self) -> impl Iterator<Item = uuid::Uuid> {
		self.0.iter().map(|entry| entry.key().clone())
	}
}

pub struct LorGuard<'a, T> {
	value: Ref<'a, uuid::Uuid, (Arc<Semaphore>, Arc<T>)>,
	_permit: OwnedSemaphorePermit,
}

impl<'a, T> LorGuard<'a, T> {
	pub fn new(value: Ref<'a, uuid::Uuid, (Arc<Semaphore>, Arc<T>)>, permit: OwnedSemaphorePermit) -> Self {
		Self { value, _permit: permit }
	}
}

impl<'a, T> std::ops::Deref for LorGuard<'a, T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Self::Target {
        &self.value.1
    }
}

pub struct LorMutGuard<'a, T> {
	value: RefMut<'a, uuid::Uuid, (Arc<Semaphore>, Arc<T>)>,
	_permit: OwnedSemaphorePermit,
}

impl<'a, T> LorMutGuard<'a, T> {
	pub fn new(value: RefMut<'a, uuid::Uuid, (Arc<Semaphore>, Arc<T>)>, permit: OwnedSemaphorePermit) -> Self {
		Self { value, _permit: permit }
	}
}

impl<'a, T> std::ops::Deref for LorMutGuard<'a, T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Self::Target {
        &self.value.1
    }
}

impl<'a, T> std::ops::DerefMut for LorMutGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut (*self.value).1
    }
}