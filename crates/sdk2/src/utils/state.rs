use std::sync::Arc;
use dashmap::DashMap;
use surrealdb_types::Variables;
use tokio::sync::RwLock;
use uuid::Uuid;

/// ConnectionState holds the URL and all sessions.
/// When the URL changes, a new ConnectionState is created, dropping all sessions.
#[derive(Clone, Default)]
pub struct ConnectionState {
    pub url: String,
    pub root_session: Arc<RwLock<SessionState>>,
    pub sessions: DashMap<Uuid, Arc<RwLock<SessionState>>>,
}

impl ConnectionState {
    pub fn new(url: String, root_session: SessionState) -> Self {
        Self {
            url,
            root_session: Arc::new(RwLock::new(root_session)),
            sessions: DashMap::new(),
        }
    }

    /// Get a session state for reading (completely lock-free).
    ///
    /// Returns a cloned Arc of the current session state snapshot.
    pub fn get_session(&self, session_id: Option<Uuid>) -> Arc<RwLock<SessionState>> {
        match session_id {
            None => self.root_session.clone(),
            Some(id) => match self.sessions.get(&id) {
                Some(entry) => entry.value().clone(),
                None => {
                    let session = Arc::new(RwLock::new(SessionState::default()));
                    self.sessions.insert(id, session.clone());
                    session
                }
            }
        }
    }

    pub fn new_session(&self) -> Uuid {
        let uuid = Uuid::now_v7();
        self.sessions.insert(uuid, Arc::new(RwLock::new(SessionState::default())));
        uuid
    }

    /// Create or update a session
    pub async fn upsert_session(&self, session_id: Option<Uuid>, state: SessionState) {
        if let Some(id) = session_id {
            self.sessions.insert(id, Arc::new(RwLock::new(state)));
        } else {
            let mut root = self.root_session.write().await;
            *root = state;
        }
    }

    /// Remove a session
    pub fn remove_session(&self, session_id: Uuid) -> Option<Arc<RwLock<SessionState>>> {
        self.sessions.remove(&session_id).map(|(_, v)| v)
    }

    /// List all session IDs
    pub fn list_sessions(&self) -> Vec<Uuid> {
        self.sessions.iter().map(|entry| *entry.key()).collect()
    }

    pub fn all_sessions(&self) -> Vec<Arc<RwLock<SessionState>>> {
        let mut sessions = vec![self.root_session.clone()];
        sessions.extend(self.sessions.iter().map(|entry| entry.value().clone()));
        sessions
    }
}

#[derive(Clone, Default)]
pub struct SessionState {
    pub id: Option<Uuid>,
    pub namespace: Option<String>,
    pub database: Option<String>,
    pub refresh_token: Option<String>,
    pub access_token: Option<String>,
    pub variables: Variables,
}