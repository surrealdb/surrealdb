use crate::ctx::Context;
use crate::dbs::Auth;
use crate::sql::value::Value;
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Session {
	pub au: Arc<Auth>,      // Authentication info
	pub ip: Option<String>, // Session ip address
	pub or: Option<String>, // Session origin
	pub id: Option<String>, // Session id
	pub ns: Option<String>, // Namespace
	pub db: Option<String>, // Database
	pub sc: Option<String>, // Scope
	pub sd: Option<Value>,  // Scope auth data
}

impl Session {
	// Retrieves the selected namespace
	pub fn ns(&self) -> Option<Arc<String>> {
		self.ns.to_owned().map(Arc::new)
	}
	// Retrieves the selected database
	pub fn db(&self) -> Option<Arc<String>> {
		self.db.to_owned().map(Arc::new)
	}
	// Convert a session into a runtime
	pub fn context(&self, mut ctx: Context) -> Context {
		// Add session value
		let key = String::from("session");
		let val: Value = self.into();
		ctx.add_value(key, val);
		// Add scope value
		let key = String::from("scope");
		let val: Value = self.sc.to_owned().into();
		ctx.add_value(key, val);
		// Add auth data
		let key = String::from("auth");
		let val: Value = self.sd.to_owned().into();
		ctx.add_value(key, val);
		// Output context
		ctx
	}
}

impl From<&Session> for Value {
	fn from(val: &Session) -> Value {
		Value::from(map! {
			"ip".to_string() => val.ip.to_owned().into(),
			"or".to_string() => val.or.to_owned().into(),
			"id".to_string() => val.id.to_owned().into(),
			"ns".to_string() => val.ns.to_owned().into(),
			"db".to_string() => val.db.to_owned().into(),
			"sc".to_string() => val.sc.to_owned().into(),
		})
	}
}
