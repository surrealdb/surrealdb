use crate::ctx::Context;
use crate::dbs::Auth;
use crate::sql::value::Value;
use std::sync::Arc;

/// Specifies the current session information when processing a query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Session {
	/// The current [`Auth`] information
	pub au: Arc<Auth>,
	/// The current connection IP address
	pub ip: Option<String>,
	/// The current connection origin
	pub or: Option<String>,
	/// The current connection ID
	pub id: Option<String>,
	/// THe currently selected namespace
	pub ns: Option<String>,
	/// THe currently selected database
	pub db: Option<String>,
	/// The currently selected authentication scope
	pub sc: Option<String>,
	/// The current scope authentication data
	pub sd: Option<Value>,
}

impl Session {
	// Retrieves the selected namespace
	pub(crate) fn ns(&self) -> Option<Arc<String>> {
		self.ns.to_owned().map(Arc::new)
	}
	// Retrieves the selected database
	pub(crate) fn db(&self) -> Option<Arc<String>> {
		self.db.to_owned().map(Arc::new)
	}
	// Convert a session into a runtime
	pub(crate) fn context<'a>(&self, mut ctx: Context<'a>) -> Context<'a> {
		// Add scope value
		let key = String::from("scope");
		let val: Value = self.sc.to_owned().into();
		ctx.add_value(key, val);
		// Add auth data
		let key = String::from("auth");
		let val: Value = self.sd.to_owned().into();
		ctx.add_value(key, val);
		// Add session value
		let key = String::from("session");
		let val: Value = Value::from(map! {
			"ip".to_string() => self.ip.to_owned().into(),
			"or".to_string() => self.or.to_owned().into(),
			"id".to_string() => self.id.to_owned().into(),
			"ns".to_string() => self.ns.to_owned().into(),
			"db".to_string() => self.db.to_owned().into(),
			"sc".to_string() => self.sc.to_owned().into(),
		});
		ctx.add_value(key, val);
		// Output context
		ctx
	}
}
