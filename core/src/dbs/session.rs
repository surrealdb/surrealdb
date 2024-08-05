use crate::ctx::Context;
use crate::iam::Auth;
use crate::iam::{Level, Role};
use crate::sql::statements::live::MaybeSession;
use crate::sql::value::Value;
use crate::sql::Uuid;
use chrono::Utc;
use std::sync::Arc;

/// Specifies the current session information when processing a query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Session {
	/// The session ID, used as a reference on KV store.
	/// Changes in auth and ns/db/sc should be reflected on KV store.
	pub sess_id: Uuid,
	/// The current session [`Auth`] information
	pub au: Arc<Auth>,
	/// Whether realtime queries are supported
	pub rt: bool,
	/// The current connection IP address
	pub ip: Option<String>,
	/// The current connection origin
	pub or: Option<String>,
	/// The current connection ID
	pub id: Option<String>,
	/// The currently selected namespace
	pub ns: Option<String>,
	/// The currently selected database
	pub db: Option<String>,
	/// The currently selected authentication scope
	pub sc: Option<String>,
	/// The current scope authentication token
	pub tk: Option<Value>,
	/// The current scope authentication data
	pub sd: Option<Value>,
	/// The current expiration time of the session
	pub exp: Option<i64>,
}

impl Session {
	/// Set the selected namespace for the session
	pub fn with_ns(mut self, ns: &str) -> Session {
		self.ns = Some(ns.to_owned());
		self
	}

	/// Set the selected database for the session
	pub fn with_db(mut self, db: &str) -> Session {
		self.db = Some(db.to_owned());
		self
	}

	/// Set the selected database for the session
	pub fn with_sc(mut self, sc: &str) -> Session {
		self.sc = Some(sc.to_owned());
		self
	}

	// Set the realtime functionality of the session
	pub fn with_rt(mut self, rt: bool) -> Session {
		self.rt = rt;
		self
	}

	/// Retrieves the selected namespace
	pub(crate) fn ns(&self) -> Option<Arc<str>> {
		self.ns.as_deref().map(Into::into)
	}

	/// Retrieves the selected database
	pub(crate) fn db(&self) -> Option<Arc<str>> {
		self.db.as_deref().map(Into::into)
	}

	/// Checks if live queries are allowed
	pub(crate) fn live(&self) -> bool {
		self.rt
	}

	/// Checks if the session has expired
	pub(crate) fn expired(&self) -> bool {
		match self.exp {
			Some(exp) => Utc::now().timestamp() > exp,
			// It is currently possible to have sessions without expiration.
			None => false,
		}
	}

	/// Convert a session into a runtime
	pub(crate) fn context<'a>(&self, mut ctx: Context<'a>) -> Context<'a> {
		// Add scope auth data
		let val: Value = self.sd.to_owned().into();
		ctx.add_value("auth", val);
		// Add scope data
		let val: Value = self.sc.to_owned().into();
		ctx.add_value("scope", val);
		// Add token data
		let val: Value = self.tk.to_owned().into();
		ctx.add_value("token", val);
		// Add session value
		let val: Value = Value::from(map! {
			"db".to_string() => self.db.to_owned().into(),
			"id".to_string() => self.id.to_owned().into(),
			"ip".to_string() => self.ip.to_owned().into(),
			"ns".to_string() => self.ns.to_owned().into(),
			"or".to_string() => self.or.to_owned().into(),
			"sc".to_string() => self.sc.to_owned().into(),
			"sd".to_string() => self.sd.to_owned().into(),
			"tk".to_string() => self.tk.to_owned().into(),
			"exp".to_string() => self.exp.to_owned().into(),
		});
		ctx.add_value("session", val);
		// Output context
		ctx
	}

	/// Create a system session for a given level and role
	pub fn for_level(level: Level, role: Role) -> Session {
		// Create a new session
		let mut sess = Session::default();
		// Set the session details
		match level {
			Level::Root => {
				sess.au = Arc::new(Auth::for_root(role));
			}
			Level::Namespace(ns) => {
				sess.au = Arc::new(Auth::for_ns(role, &ns));
				sess.ns = Some(ns);
			}
			Level::Database(ns, db) => {
				sess.au = Arc::new(Auth::for_db(role, &ns, &db));
				sess.ns = Some(ns);
				sess.db = Some(db);
			}
			_ => {}
		}
		sess
	}

	/// Create a scoped session for a given NS and DB
	pub fn for_scope(ns: &str, db: &str, sc: &str, rid: Value) -> Session {
		Session {
			au: Arc::new(Auth::for_sc(rid.to_string(), ns, db, sc)),
			rt: false,
			ip: None,
			or: None,
			id: None,
			ns: Some(ns.to_owned()),
			db: Some(db.to_owned()),
			sc: Some(sc.to_owned()),
			tk: None,
			sd: Some(rid),
			exp: None,
			sess_id: Uuid::default(),
		}
	}

	/// Create a system session for the root level with Owner role
	pub fn owner() -> Session {
		Session::for_level(Level::Root, Role::Owner)
	}

	/// Create a system session for the root level with Editor role
	pub fn editor() -> Session {
		Session::for_level(Level::Root, Role::Editor)
	}

	/// Create a system session for the root level with Viewer role
	pub fn viewer() -> Session {
		Session::for_level(Level::Root, Role::Viewer)
	}
}
