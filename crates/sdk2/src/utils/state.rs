use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};
use surrealdb_types::Variables;
use uuid::Uuid;

#[derive(Clone)]
pub struct ConnectionState {
    pub url: String,
    root_session: Arc<SessionContainer>,
    sessions: DashMap<Uuid, Arc<SessionContainer>>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            url: String::new(),
            root_session: Arc::new(SessionContainer::new(SessionState::default())),
            sessions: DashMap::new(),
        }
    }
}

impl ConnectionState {
    pub fn new(url: String, root_session: SessionState) -> Self {
        Self {
            url,
            root_session: Arc::new(SessionContainer::new(root_session)),
            sessions: DashMap::new(),
        }
    }

    /// Lock-free read
    pub fn get_session(&self, session_id: Option<Uuid>) -> Arc<SessionState> {
        self.container(session_id).load()
    }

    /// Exclusive write access - commits on drop
    pub fn get_session_mut(&self, session_id: Option<Uuid>) -> SessionWriteGuard {
        SessionContainer::write(self.container(session_id))
    }

    fn container(&self, session_id: Option<Uuid>) -> Arc<SessionContainer> {
        match session_id {
            None => self.root_session.clone(),
            Some(id) => self.sessions
                .entry(id)
                .or_insert_with(|| Arc::new(SessionContainer::new(SessionState::new(Some(id)))))
                .clone()
        }
    }

    pub fn new_session(&self) -> Uuid {
        let uuid = Uuid::now_v7();
        self.sessions.insert(uuid, Arc::new(SessionContainer::new(SessionState::new(Some(uuid)))));
        uuid
    }

    pub fn fork_session(&self, session_id: Option<Uuid>) -> Uuid {
        let mut session = (*self.get_session(session_id)).clone();
        let uuid = Uuid::now_v7();
        session.id = Some(uuid);
        self.sessions.insert(uuid, Arc::new(SessionContainer::new(session)));
        uuid
    }

    pub fn remove_session(&self, session_id: Uuid) -> Option<Arc<SessionState>> {
        self.sessions.remove(&session_id).map(|(_, c)| c.load())
    }

    pub fn list_sessions(&self) -> Vec<Uuid> {
        self.sessions.iter().map(|e| *e.key()).collect()
    }

    pub fn all_sessions(&self) -> Vec<Arc<SessionState>> {
        let mut sessions = vec![self.root_session.load()];
        sessions.extend(self.sessions.iter().map(|e| e.value().load()));
        sessions
    }
}

// --- Internal ---

struct SessionContainer {
    state: ArcSwap<SessionState>,
    write_lock: Mutex<()>,
}

impl SessionContainer {
    fn new(state: SessionState) -> Self {
        Self {
            state: ArcSwap::from_pointee(state),
            write_lock: Mutex::new(()),
        }
    }

    fn load(&self) -> Arc<SessionState> {
        self.state.load_full()
    }

    fn write(container: Arc<Self>) -> SessionWriteGuard {
        // SAFETY: We hold the Arc, so the container lives as long as the guard.
        // We need to acquire the lock before creating the guard.
        let guard = unsafe { &*Arc::as_ptr(&container) }.write_lock.lock();
        let state = (*container.state.load_full()).clone();
        SessionWriteGuard {
            container,
            state: Some(state),
            _guard: guard,
        }
    }
}

pub struct SessionWriteGuard {
    container: Arc<SessionContainer>,
    state: Option<SessionState>,
    // MutexGuard is tied to the Mutex inside container, which lives in the Arc
    _guard: MutexGuard<'static, ()>,
}

impl SessionWriteGuard {
    /// Discard changes without committing
    pub fn abort(mut self) {
        self.state = None;
    }
}

impl Deref for SessionWriteGuard {
    type Target = SessionState;
    fn deref(&self) -> &Self::Target {
        self.state.as_ref().unwrap()
    }
}

impl DerefMut for SessionWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state.as_mut().unwrap()
    }
}

impl Drop for SessionWriteGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            self.container.state.store(Arc::new(state));
        }
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

impl SessionState {
    pub fn new(id: Option<Uuid>) -> Self {
        Self { id, ..Default::default() }
    }

    pub fn reset(&mut self) {
        let id = self.id.take();
        *self = Self { id, ..Default::default() };
    }
}
