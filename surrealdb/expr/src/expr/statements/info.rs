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
impl InfoStatement {
	/// Whether evaluating this statement does only reads.
	///
	/// `INFO` reads catalog metadata, but its subject and `VERSION` clause are
	/// expressions the statement evaluates first, so a write reaches the
	/// transaction through them.
	pub fn read_only(&self) -> bool {
		match self {
			InfoStatement::Root(_, version)
			| InfoStatement::Ns(_, version)
			| InfoStatement::Db(_, version) => version.as_ref().is_none_or(|version| version.read_only()),
			InfoStatement::Tb(table, _, version) => {
				table.read_only() && version.as_ref().is_none_or(|version| version.read_only())
			}
			InfoStatement::User(user, _, _) => user.read_only(),
			InfoStatement::Index(index, table, _) => index.read_only() && table.read_only(),
		}
	}
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
