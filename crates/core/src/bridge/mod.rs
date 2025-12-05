
// pub struct NamespaceDatabase {
//     pub namespace: Nullable<String>,
//     pub database: Nullable<String>,
// }

// pub enum Nullable<T> {
//     Some(T),
//     Null,
//     None,
// }

// /// A guard that holds both the Arc (to keep it alive) and the write guard
// /// This ensures the lock stays valid while the guard is held
// pub struct SessionMutGuard<'a> {
//     pub value: RefMut<'a, Option<Uuid>, Arc<Session>>,
//     pub permit: OwnedSemaphorePermit,
// }

// impl<'a> std::ops::Deref for SessionMutGuard<'a> {
//     type Target = Arc<Session>;

//     fn deref(&self) -> &Self::Target {
//         &*self.value
//     }
// }

// impl<'a> std::ops::DerefMut for SessionMutGuard<'a> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut *self.value
//     }
// }

// pub struct Bridge {
//     kvs: Arc<Datastore>,
//     sessions: DashMap<Option<Uuid>, Arc<Session>>,
//     locks: DashMap<Option<Uuid>, Arc<Semaphore>>,
//     transactions: DashMap<Uuid, Arc<Transaction>>,
//     live_queries: DashMap<Uuid, Option<Uuid>>,
// }

// impl Bridge {
//     pub fn new(kvs: Arc<Datastore>) -> Self {
//         Self {
//             kvs,
//             sessions: DashMap::new(),
//             locks: DashMap::new(),
//             transactions: DashMap::new(),
//             live_queries: DashMap::new(),
//         }
//     }

//     pub async fn connect(endpoint: String, opts: Option<Options>) -> Result<Bridge> {
//         let endpoint = match &endpoint {
//             s if s.starts_with("mem:") => "memory",
//             s => s,
//         };

//         let kvs = Datastore::new(endpoint).await?.with_notifications();
//         let kvs = match opts {
//             None => kvs,
//             Some(opts) => kvs
//                 .with_capabilities(
//                     opts.capabilities
//                         .map_or(Ok(Default::default()), |a| a.try_into())?,
//                 )
//                 .with_transaction_timeout(
//                     opts.transaction_timeout
//                         .map(|qt| std::time::Duration::from_secs(qt as u64)),
//                 )
//                 .with_query_timeout(
//                     opts.query_timeout
//                         .map(|qt| std::time::Duration::from_secs(qt as u64)),
//                 )
//                 .with_strict_mode(opts.strict.map_or(Default::default(), |s| s)),
//         };

//         Ok(Bridge::new(Arc::new(kvs)))
//     }

//     pub fn kvs(&self) -> &Datastore {
//         self.kvs.as_ref()
//     }

//     pub fn sessions(&self) -> Vec<Uuid> {
//         self.sessions
//             .iter()
//             .filter_map(|entry| *entry.key())
//             .collect()
//     }

//     pub fn get_session(&self, session_id: Option<Uuid>) -> Arc<Session> {
//         self.sessions
//             .entry(session_id)
//             .or_insert_with(|| Arc::new(Session::default()))
//             .clone()
//     }

//     pub async fn get_session_mut(&self, session_id: Option<Uuid>) -> Result<SessionMutGuard<'_>> {
//         let lock = self
//             .locks
//             .entry(session_id)
//             .or_insert_with(|| Arc::new(Semaphore::new(1)))
//             .clone();
//         let permit = lock.clone().acquire_owned().await?;
//         let value = self
//             .sessions
//             .entry(session_id)
//             .or_insert_with(|| Arc::new(Session::default()));

//         Ok(SessionMutGuard { value, permit })
//     }

//     pub async fn yuse(&self, session_id: Option<Uuid>, ns_db: NamespaceDatabase) -> Result<()> {
//         let mut session = self.get_session_mut(session_id).await?;

//         let mut new = session.as_ref().clone();

//         match ns_db.namespace {
//             Nullable::Some(namespace) => {
//                 new.ns = Some(namespace);
//             }
//             Nullable::Null => {
//                 new.ns = None;
//             }
//             Nullable::None => {}
//         }

//         match ns_db.database {
//             Nullable::Some(database) => {
//                 new.db = Some(database);
//             }
//             Nullable::Null => {
//                 new.db = None;
//             }
//             Nullable::None => {}
//         }

//         if new.ns.is_none() && new.db.is_some() {
//             new.db = None;
//         }

//         *session = Arc::new(new);
//         Ok(())
//     }

//     pub fn version(&self) -> String {
//         env!("SURREALDB_VERSION").into()
//     }

//     pub async fn signup(&self, session_id: Option<Uuid>, params: Variables) -> Result<Value> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();

//         let out: Value = iam::signup::signup(self.kvs(), &mut new, params)
//             .await
//             .map(SurrealValue::into_value)?;

//         *session = Arc::new(new);
//         Ok(out)
//     }

//     pub async fn signin(&self, session_id: Option<Uuid>, params: Variables) -> Result<Value> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();

//         let out: Value = iam::signin::signin(self.kvs(), &mut new, params)
//             .await
//             .map(SurrealValue::into_value)?;

//         *session = Arc::new(new);
//         Ok(out)
//     }

//     pub async fn authenticate(&self, session_id: Option<Uuid>, token: String) -> Result<()> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();

//         iam::verify::token(self.kvs(), &mut new, &token).await?;

//         *session = Arc::new(new);
//         Ok(())
//     }

//     pub async fn set(&self, session_id: Option<Uuid>, name: String, value: Value) -> Result<()> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();

//         new.variables.insert(name, value);
//         *session = Arc::new(new);

//         Ok(())
//     }

//     pub async fn unset(&self, session_id: Option<Uuid>, name: String) -> Result<()> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();

//         new.variables.remove(&name);
//         *session = Arc::new(new);
//         Ok(())
//     }

//     pub async fn refresh(&self, session_id: Option<Uuid>, tokens: Value) -> Result<Value> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();
//         let token = Token::from_value(tokens)?;
//         let out = token.refresh(self.kvs(), &mut new).await?;
//         *session = Arc::new(new);

//         Ok(out.into_value())
//     }

//     pub async fn revoke(&self, tokens: Value) -> Result<()> {
//         let token = Token::from_value(tokens)?;
//         token.revoke_refresh_token(self.kvs()).await?;
//         Ok(())
//     }

//     pub async fn invalidate(&self, session_id: Option<Uuid>) -> Result<()> {
//         let mut session = self.get_session_mut(session_id).await?;
//         let mut new = session.as_ref().clone();
//         iam::clear::clear(&mut new)?;
//         *session = Arc::new(new);
//         Ok(())
//     }

//     pub async fn reset(&self, session_id: Option<Uuid>) -> Result<()> {
//         if session_id.is_some() {
//             let lock = self
//                 .locks
//                 .entry(session_id)
//                 .or_insert_with(|| Arc::new(Semaphore::new(1)))
//                 .clone();
//             let _ = lock.clone().acquire_owned().await?;
//             self.sessions.remove(&session_id);
//             self.locks.remove(&session_id);
//         } else {
//             let mut session = self.get_session_mut(None).await?;
//             let mut new = session.as_ref().clone();
//             iam::reset::reset(&mut new);
//             *session = Arc::new(new);
//         }

//         Ok(())
//     }

//     pub async fn begin(&self) -> Result<Uuid> {
//         // Create a new transaction
//         let tx = self
//             .kvs()
//             .transaction(TransactionType::Write, LockType::Optimistic)
//             .await?;
//         // Generate a unique transaction ID
//         let id = Uuid::now_v7();
//         // Persist
//         self.transactions.insert(id, Arc::new(tx));
//         Ok(id)
//     }

//     pub async fn commit(&self, txn: Uuid) -> Result<()> {
//         let Some((_, tx)) = self.transactions.remove(&txn) else {
//             return Err(anyhow::anyhow!("Transaction not found"));
//         };

//         tx.commit().await?;
//         Ok(())
//     }

//     pub async fn cancel(&self, txn: Uuid) -> Result<()> {
//         let Some((_, tx)) = self.transactions.remove(&txn) else {
//             return Err(anyhow::anyhow!("Transaction not found"));
//         };

//         tx.cancel().await?;
//         Ok(())
//     }

//     pub async fn import(&self, session_id: Option<Uuid>, sql: String) -> Result<()> {
//         let session = self.get_session(session_id);
//         self.kvs().import(&sql, session.as_ref()).await?;
//         Ok(())
//     }

//     pub async fn export(&self, session_id: Option<Uuid>, config: Value) -> Result<String> {
//         let config = surrealdb_core::kvs::export::Config::from_value(config)?;
//         let session = self.get_session(session_id);
//         let (tx, rx) = async_channel::bounded(1);

//         self.kvs()
//             .export_with_config(session.as_ref(), tx, config)
//             .await?
//             .await?;

//         let sql = rx.recv().await?;
//         let sql = String::from_utf8(sql)?;

//         Ok(sql)
//     }

//     pub async fn query(
//         &self,
//         session_id: Option<Uuid>,
//         txn: Option<Uuid>,
//         query: String,
//         vars: Variables,
//     ) -> Result<Vec<Value>> {
//         let session = self.get_session(session_id);

//         let res = if let Some(txn_id) = txn {
//             let Some(tx) = self.transactions.get(&txn_id) else {
//                 return Err(anyhow::anyhow!("Transaction not found"));
//             };

//             self.kvs()
//                 .execute_with_transaction(&query, &session, Some(vars), tx.clone())
//                 .await?
//         } else {
//             self.kvs().execute(&query, &session, Some(vars)).await?
//         };

//         for response in &res {
//             match &response.query_type {
//                 QueryType::Live => {
//                     if let Ok(Value::Uuid(lqid)) = &response.result {
//                         self.live_queries.insert(lqid.0, session_id);
//                     }
//                 }
//                 QueryType::Kill => {
//                     if let Ok(Value::Uuid(lqid)) = &response.result {
//                         self.live_queries.remove(&lqid.0);
//                     }
//                 }
//                 _ => {}
//             }
//         }

//         Ok(res.into_iter().map(SurrealValue::into_value).collect())
//     }

//     pub fn notifications(&self) -> Result<Receiver<Notification>> {
//         let Some(stream) = self.kvs().notifications() else {
//             bail!("Notifications not enabled");
//         };

//         Ok(stream)
//     }
// }

use std::sync::Arc;

use crate::dbs::Session;
use crate::iam::Token;
use crate::kvs::export::Config;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::types::{PublicValue, PublicNotification, PublicVariables, PublicDuration};
use anyhow::{bail, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use uuid::Uuid;

#[async_trait::async_trait]
pub trait SurrealBridge {
    // Connection
    async fn health(&self) -> Result<()>;
    async fn version(&self) -> Result<String>;

    // Sessions
    async fn new_session(&self) -> Result<Uuid>;
    async fn get_session(&self, session_id: Uuid) -> Result<Option<PublicSession>>;
    async fn drop_session(&self, session_id: Uuid) -> Result<()>;
    async fn reset_session(&self, session_id: Uuid) -> Result<()>;
    async fn list_sessions(&self) -> Result<Vec<Uuid>>;

    // Session modifiers
    async fn r#use(&self, session_id: Uuid, ns: Option<String>, db: Option<String>) -> Result<()>;
    async fn set_variable(&self, session_id: Uuid, name: String, value: PublicValue) -> Result<()>;
    async fn drop_variable(&self, session_id: Uuid, name: String) -> Result<()>;

    // Transactions
    async fn begin_transaction(&self, session_id: Uuid) -> Result<Uuid>;
    async fn commit_transaction(&self, session_id: Uuid, transaction_id: Uuid) -> Result<()>;
    async fn cancel_transaction(&self, session_id: Uuid, transaction_id: Uuid) -> Result<()>;
    async fn list_transactions(&self, session_id: Uuid) -> Result<Vec<Uuid>>;

    // Authentication
    async fn signup(&self, session_id: Uuid, params: PublicVariables) -> Result<PublicTokens>;
    async fn signin(&self, session_id: Uuid, params: PublicVariables) -> Result<PublicTokens>;
    async fn authenticate(&self, session_id: Uuid, token: String) -> Result<()>;
    async fn refresh(&self, session_id: Uuid, tokens: PublicTokens) -> Result<PublicTokens>;
    async fn revoke(&self, tokens: PublicTokens) -> Result<()>;
    async fn invalidate(&self, session_id: Uuid) -> Result<()>;

    // Export & Import
    // Should these return & accept a stream of bytes in the trait?
    async fn export(&self, session_id: Uuid, config: Config) -> Result<impl Stream<Item = Bytes>>;
    async fn import<S: Stream<Item = anyhow::Result<Bytes>> + Send + 'static>(&self, session_id: Uuid, sql: S) -> Result<()>;

    // Query
    async fn query(&self, session_id: Uuid, txn: Option<Uuid>, query: String, vars: PublicVariables) -> Result<impl Stream<Item = QueryChunk>>;

    // Live notifications
    // unfiltered, the implementor can pipe these notifications to the proper recipient
    async fn notifications(&self) -> Result<impl Stream<Item = PublicNotification>>;
}

// TBA
pub struct PublicTokens {
    pub access: Option<String>,
    pub refresh: Option<String>,
}

impl From<Token> for PublicTokens {
    fn from(token: Token) -> Self {
        match token {
            Token::Access(access) => Self {
                access: Some(access),
                refresh: None,
            },
            Token::WithRefresh { access, refresh } => Self {
                access: Some(access),
                refresh: Some(refresh),
            },
        }
    }
}

impl TryInto<Token> for PublicTokens {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<Token, Self::Error> {
        match self {
            PublicTokens { access: Some(access), refresh: None } => Ok(Token::Access(access)),
            PublicTokens { access: Some(access), refresh: Some(refresh) } => Ok(Token::WithRefresh { access, refresh }),
            _ => bail!("No tokens provided"),
        }
    }
}

pub struct PublicSession {
    pub id: Uuid,
    pub ns: Option<String>,
    pub db: Option<String>,
    pub variables: PublicVariables,
}

#[async_trait::async_trait]
impl SurrealBridge for Datastore {
    async fn health(&self) -> Result<()> {
        self.health_check().await
    }

    async fn version(&self) -> Result<String> {
        // TODO: datastore doesnt expose version. Should we include this in the build process?
        Ok("0.0.0".to_string())
    }

    async fn new_session(&self) -> Result<Uuid> {
        let id = Uuid::now_v7();
        self.get_sessions().insert(id, Arc::new(Session::default())).await?;
        Ok(id)
    }

    async fn get_session(&self, session_id: Uuid) -> Result<Option<PublicSession>> {
        let Some(session) = self.get_sessions().get(&session_id) else {
            return Ok(None);
        };

        Ok(Some(PublicSession {
            id: session_id,
            ns: session.ns.clone(),
            db: session.db.clone(),
            variables: session.variables.clone(),
        }))
    }

    async fn drop_session(&self, session_id: Uuid) -> Result<()> {
        self.get_sessions().remove(&session_id).await?;
        Ok(())
    }

    async fn reset_session(&self, session_id: Uuid) -> Result<()> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();

        crate::iam::reset::reset(&mut new);
        *session = Arc::new(new);

        Ok(())
    }

    async fn list_sessions(&self) -> Result<Vec<Uuid>> {
        Ok(self.get_sessions().keys().collect())
    }
    
    async fn r#use(&self, session_id: Uuid, ns: Option<String>, db: Option<String>) -> Result<()> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();

        new.ns = ns;
        new.db = db;

        if new.ns.is_none() && new.db.is_some() {
            new.db = None;
        }

        *session = Arc::new(new);

        Ok(())
    }

    async fn set_variable(&self, session_id: Uuid, name: String, value: PublicValue) -> Result<()> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();
        new.variables.insert(name, value);
        *session = Arc::new(new);
        Ok(())
    }

    async fn drop_variable(&self, session_id: Uuid, name: String) -> Result<()> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            return Ok(());
        };

        let mut new = session.as_ref().clone();
        new.variables.remove(&name);
        *session = Arc::new(new);
        Ok(())
    }

    async fn begin_transaction(&self, session_id: Uuid) -> Result<Uuid> {
        if self.get_sessions().get(&session_id).is_none() {
        // TODO transactions should live under sessions probably
            bail!("Session not found");
        };

        let tx = self.transaction(TransactionType::Write, LockType::Optimistic).await?;
        let id = Uuid::now_v7();
        self.get_transactions().insert(id, Arc::new(tx)).await?;
        Ok(id)
    }

    async fn commit_transaction(&self, session_id: Uuid, transaction_id: Uuid) -> Result<()> {
        if self.get_sessions().get(&session_id).is_none() {
            // TODO transactions should live under sessions probably
            bail!("Session not found");
        };

        let Some(tx) = self.get_transactions().remove(&transaction_id).await? else {
            bail!("Transaction not found");
        };

        tx.commit().await?;
        Ok(())
    }
    
    async fn cancel_transaction(&self, session_id: Uuid, transaction_id: Uuid) -> Result<()> {
        if self.get_sessions().get(&session_id).is_none() {
            // TODO transactions should live under sessions probably
            bail!("Session not found");
        };

        let Some(tx) = self.get_transactions().remove(&transaction_id).await? else {
            bail!("Transaction not found");
        };

        tx.cancel().await?;
        Ok(())
    }

    async fn list_transactions(&self, session_id: Uuid) -> Result<Vec<Uuid>> {
        if self.get_sessions().get(&session_id).is_none() {
            bail!("Session not found");
        };

        Ok(self.get_transactions().keys().collect())
    }

    async fn signup(&self, session_id: Uuid, params: PublicVariables) -> Result<PublicTokens> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();

        let out = crate::iam::signup::signup(self, &mut new, params).await?;
        *session = Arc::new(new);

        Ok(out.into())
    }

    async fn signin(&self, session_id: Uuid, params: PublicVariables) -> Result<PublicTokens> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();

        let out = crate::iam::signin::signin(self, &mut new, params).await?;
        *session = Arc::new(new);

        Ok(out.into())
    }

    async fn authenticate(&self, session_id: Uuid, token: String) -> Result<()> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();

        crate::iam::verify::token(self, &mut new, &token).await?;
        *session = Arc::new(new);

        Ok(())
    }

    async fn refresh(&self, session_id: Uuid, tokens: PublicTokens) -> Result<PublicTokens> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();
        let token: Token = tokens.try_into()?;
        let out = token.refresh(self, &mut new).await?;
        *session = Arc::new(new);

        Ok(out.into())
    }

    async fn revoke(&self, tokens: PublicTokens) -> Result<()> {
        let token: Token = tokens.try_into()?;
        token.revoke_refresh_token(self).await?;
        Ok(())
    }

    async fn invalidate(&self, session_id: Uuid) -> Result<()> {
        let Some(mut session) = self.get_sessions().get_mut(&session_id).await? else {
            bail!("Session not found");
        };

        let mut new = session.as_ref().clone();
        crate::iam::clear::clear(&mut new)?;
        *session = Arc::new(new);

        Ok(())
    }

    async fn export(&self, session_id: Uuid, config: Config) -> Result<impl Stream<Item = Bytes>> {
        let Some(session) = self.get_sessions().get(&session_id) else {
            bail!("Session not found");
        };

        let (chn, rcv) = crate::channel::bounded(1);
        self.export_with_config(session.as_ref(), chn, config).await?.await?;

        Ok(rcv.map(Bytes::from))
    }

    async fn import<S: Stream<Item = anyhow::Result<Bytes>> + Send + 'static>(&self, session_id: Uuid, sql: S) -> Result<()> {
        let Some(session) = self.get_sessions().get(&session_id) else {
            bail!("Session not found");
        };

        self.import_stream(session.as_ref(), sql).await?;
        Ok(())
    }

    async fn query(&self, session_id: Uuid, txn: Option<Uuid>, query: String, vars: PublicVariables) -> Result<impl Stream<Item = QueryChunk>> {
        let Some(session) = self.get_sessions().get(&session_id) else {
            bail!("Session not found");
        };

        let res = if let Some(txn_id) = txn {
            let Some(tx) = self.get_transactions().get(&txn_id).await? else {
                return Err(anyhow::anyhow!("Transaction not found"));
            };

            self.execute_with_transaction(&query, session.as_ref(), Some(vars), tx.clone())
                .await?
        } else {
            self.execute(&query, &session, Some(vars)).await?
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
                Err(e) => (None, Some(anyhow::anyhow!(e.to_string()))),
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

        Ok(futures::stream::iter(chunks))
    }

    async fn notifications(&self) -> Result<impl Stream<Item = PublicNotification>> {
        let Some(stream) = self.notifications() else {
            bail!("Notifications not enabled");
        };

        Ok(stream.map(PublicNotification::from))
    }
}

pub struct QueryStats {
    pub records_received: u64,
    pub bytes_received: u64,
    pub records_scanned: u64,
    pub bytes_scanned: u64,
    pub duration: PublicDuration,
}

pub enum QueryResponseKind {
    Single,
    Batched,
    BatchedFinal,
}

pub enum QueryType {
    Other,
    Live,
    Kill,
}

pub struct QueryChunk {
    pub query: u64,
    pub batch: u64,
    pub kind: QueryResponseKind,
    pub stats: Option<QueryStats>,
    pub result: Option<Vec<PublicValue>>,
    pub r#type: Option<QueryType>,
    pub error: Option<anyhow::Error>,
}