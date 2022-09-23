use crate::ctx::Context;
use crate::dbs::Auth;
use crate::sql::value::Value;
use std::sync::Arc;

/// Specifies the current session information when processing a query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Session {
	/// The current [`Auth`] information
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
}

impl Session {
	/// Create a session with root authentication
	pub fn for_kv() -> Session {
		Session {
			au: Arc::new(Auth::Kv),
			..Session::default()
		}
	}
	/// Create a session with namespace authentication
	pub fn for_ns<S>(ns: S) -> Session
	where
		S: Into<String> + Clone,
	{
		Session {
			ns: Some(ns.clone().into()),
			au: Arc::new(Auth::Ns(ns.into())),
			..Session::default()
		}
	}
	/// Create a session with database authentication
	pub fn for_db<S>(ns: S, db: S) -> Session
	where
		S: Into<String> + Clone,
	{
		Session {
			ns: Some(ns.clone().into()),
			db: Some(db.clone().into()),
			au: Arc::new(Auth::Db(ns.into(), db.into())),
			..Session::default()
		}
	}
	/// Create a session with scope authentication
	pub fn for_sc<S>(ns: S, db: S, sc: S) -> Session
	where
		S: Into<String> + Clone,
	{
		Session {
			ns: Some(ns.clone().into()),
			db: Some(db.clone().into()),
			sc: Some(sc.clone().into()),
			au: Arc::new(Auth::Sc(ns.into(), db.into(), sc.into())),
			..Session::default()
		}
	}
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
	/// Retrieves the selected namespace
	pub(crate) fn ns(&self) -> Option<Arc<str>> {
		self.ns.as_deref().map(Into::into)
	}
	/// Retrieves the selected database
	pub(crate) fn db(&self) -> Option<Arc<str>> {
		self.db.as_deref().map(Into::into)
	}
	/// Convert a session into a runtime
	pub(crate) fn context<'a>(&self, mut ctx: Context<'a>) -> Context<'a> {
		// Add auth data
		let key = String::from("auth");
		let val: Value = self.sd.to_owned().into();
		ctx.add_value(key, val);
		// Add scope data
		let key = String::from("scope");
		let val: Value = self.sc.to_owned().into();
		ctx.add_value(key, val);
		// Add token data
		let key = String::from("token");
		let val: Value = self.tk.to_owned().into();
		ctx.add_value(key, val);
		// Add session value
		let key = String::from("session");
		let val: Value = Value::from(map! {
			"db".to_string() => self.db.to_owned().into(),
			"id".to_string() => self.id.to_owned().into(),
			"ip".to_string() => self.ip.to_owned().into(),
			"ns".to_string() => self.ns.to_owned().into(),
			"or".to_string() => self.or.to_owned().into(),
			"sc".to_string() => self.sc.to_owned().into(),
			"sd".to_string() => self.sd.to_owned().into(),
			"tk".to_string() => self.tk.to_owned().into(),
		});
		ctx.add_value(key, val);
		// Output context
		ctx
	}
}
