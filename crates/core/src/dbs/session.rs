use std::sync::Arc;

use chrono::Utc;
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::iam::{Auth, Level, Role};
use crate::types::{PublicValue, PublicVariables};
use crate::val::Value;

/// Specifies the current session information when processing a query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Session {
	/// The current session [`Auth`] information
	pub au: Arc<Auth>,
	/// Whether realtime queries are supported
	pub rt: bool,
	/// The current connection IP address
	pub ip: Option<String>,
	/// The current connection origin
	pub or: Option<String>,
	/// The current session ID
	pub id: Option<Uuid>,
	/// The currently selected namespace
	pub ns: Option<String>,
	/// The currently selected database
	pub db: Option<String>,
	/// The current access method
	pub ac: Option<String>,
	/// The current authentication token
	pub tk: Option<PublicValue>,
	/// The current record authentication data
	pub rd: Option<PublicValue>,
	/// The current expiration time of the session
	pub exp: Option<i64>,
	/// The variables set
	pub variables: PublicVariables,
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

	/// Set the selected access method for the session
	pub fn with_ac(mut self, ac: &str) -> Session {
		self.ac = Some(ac.to_owned());
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

	pub(crate) fn values(&self) -> Vec<(&'static str, Value)> {
		use crate::sql::expression::convert_public_value_to_internal;

		let access = self.ac.clone().map(|x| x.into()).unwrap_or(Value::None);
		let auth = self.rd.clone().map(convert_public_value_to_internal).unwrap_or(Value::None);
		let token = self.tk.clone().map(convert_public_value_to_internal).unwrap_or(Value::None);
		let session = Value::from(map! {
			"ac".to_string() => access.clone(),
			"exp".to_string() => self.exp.map(Value::from).unwrap_or(Value::None),
			"db".to_string() => self.db.clone().map(|x| x.into()).unwrap_or(Value::None),
			"id".to_string() => self.id.map(|x| Value::Uuid(x.into())).unwrap_or(Value::None),
			"ip".to_string() => self.ip.clone().map(|x| x.into()).unwrap_or(Value::None),
			"ns".to_string() => self.ns.clone().map(|x| x.into()).unwrap_or(Value::None),
			"or".to_string() => self.or.clone().map(|x| x.into()).unwrap_or(Value::None),
			"rd".to_string() => auth.clone(),
			"tk".to_string() => token.clone(),
		});

		vec![("access", access), ("auth", auth), ("token", token), ("session", session)]
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

	/// Create a record user session for a given NS and DB
	pub fn for_record(ns: &str, db: &str, ac: &str, rid: PublicValue) -> Session {
		Session {
			ac: Some(ac.to_owned()),
			au: Arc::new(Auth::for_record(rid.to_sql(), ns, db, ac)),
			rt: false,
			ip: None,
			or: None,
			id: None,
			ns: Some(ns.to_owned()),
			db: Some(db.to_owned()),
			tk: None,
			rd: Some(rid),
			exp: None,
			variables: Default::default(),
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
