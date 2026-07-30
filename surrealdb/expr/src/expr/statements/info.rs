use std::sync::Arc;

use crate::expr::{Base, Expr};
use crate::val::{Object, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum InfoStatement {
	/// Root information
	Root(bool, Option<Expr>),
	/// Namespace information
	Ns(bool, Option<Expr>),
	/// Database information
	Db(bool, Option<Expr>),
	/// Table information
	Tb(Expr, bool, Option<Expr>),

	User(Expr, Option<Base>, bool),
	/// Index information
	Index(Expr, Expr, bool),
}
pub trait InfoStructure {
	fn structure(self) -> Value;
}

impl InfoStructure for surrealdb_cnf::DynamicConfiguration {
	/// Expose the dynamic configuration as a value for the `INFO` statement.
	fn structure(self) -> Value {
		let object = map! {
			"QUERY_TIMEOUT" => match self.get_query_timeout() {
				None => Value::None,
				Some(d) => d.into(),
			}
		};
		Value::Object(Object::from(object))
	}
}

pub fn process<T>(a: &Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
