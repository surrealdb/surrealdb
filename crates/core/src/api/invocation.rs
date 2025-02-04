use super::method::Method;
use crate::{
	dbs::Session,
	sql::{Object, Value},
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiInvocation<'a> {
	pub params: Object,
	pub method: Method,
	pub query: Object,
	pub headers: Object,
	pub session: Option<Session>,
	pub values: Vec<(&'a str, Value)>,
}

impl<'a> ApiInvocation<'a> {
	pub fn vars(self, body: Value) -> Value {
		let mut obj = map! {
			"params" => Value::from(self.params),
			"body" => body,
			"method" => self.method.to_string().into(),
			"query" => Value::from(self.query),
			"headers" => Value::from(self.headers),
		};

		if let Some(session) = self.session {
			obj.extend(session.values().into_iter());
		}

		obj.extend(self.values.into_iter());

		obj.into()
	}
}
