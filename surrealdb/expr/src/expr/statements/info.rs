use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{Base, CoverStmts, Expr};
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

impl ToSql for InfoStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Root(false, v) => match v {
				Some(v) => write_sql!(f, sql_fmt, "INFO FOR ROOT VERSION {}", CoverStmts(v)),
				None => f.push_str("INFO FOR ROOT"),
			},
			Self::Root(true, v) => match v {
				Some(v) => {
					write_sql!(f, sql_fmt, "INFO FOR ROOT VERSION {} STRUCTURE", CoverStmts(v))
				}
				None => f.push_str("INFO FOR ROOT STRUCTURE"),
			},
			Self::Ns(false, v) => match v {
				Some(v) => {
					write_sql!(f, sql_fmt, "INFO FOR NAMESPACE VERSION {}", CoverStmts(v))
				}
				None => f.push_str("INFO FOR NAMESPACE"),
			},
			Self::Ns(true, v) => match v {
				Some(v) => {
					write_sql!(f, sql_fmt, "INFO FOR NAMESPACE VERSION {} STRUCTURE", CoverStmts(v))
				}
				None => f.push_str("INFO FOR NAMESPACE STRUCTURE"),
			},
			Self::Db(false, v) => match v {
				Some(v) => write_sql!(f, sql_fmt, "INFO FOR DATABASE VERSION {}", CoverStmts(v)),
				None => f.push_str("INFO FOR DATABASE"),
			},
			Self::Db(true, v) => match v {
				Some(v) => {
					write_sql!(f, sql_fmt, "INFO FOR DATABASE VERSION {} STRUCTURE", CoverStmts(v))
				}
				None => f.push_str("INFO FOR DATABASE STRUCTURE"),
			},
			Self::Tb(t, false, v) => match v {
				Some(v) => {
					write_sql!(
						f,
						sql_fmt,
						"INFO FOR TABLE {} VERSION {}",
						CoverStmts(t),
						CoverStmts(v)
					)
				}
				None => write_sql!(f, sql_fmt, "INFO FOR TABLE {}", CoverStmts(t)),
			},
			Self::Tb(t, true, v) => match v {
				Some(v) => write_sql!(
					f,
					sql_fmt,
					"INFO FOR TABLE {} VERSION {} STRUCTURE",
					CoverStmts(t),
					CoverStmts(v)
				),
				None => write_sql!(f, sql_fmt, "INFO FOR TABLE {} STRUCTURE", CoverStmts(t)),
			},
			Self::User(u, b, false) => match b {
				Some(b) => write_sql!(f, sql_fmt, "INFO FOR USER {} ON {b}", CoverStmts(u)),
				None => write_sql!(f, sql_fmt, "INFO FOR USER {}", CoverStmts(u)),
			},
			Self::User(u, b, true) => match b {
				Some(b) => {
					write_sql!(f, sql_fmt, "INFO FOR USER {} ON {b} STRUCTURE", CoverStmts(u))
				}
				None => write_sql!(f, sql_fmt, "INFO FOR USER {} STRUCTURE", CoverStmts(u)),
			},
			Self::Index(i, t, false) => {
				write_sql!(f, sql_fmt, "INFO FOR INDEX {} ON {}", CoverStmts(i), CoverStmts(t))
			}
			Self::Index(i, t, true) => {
				write_sql!(
					f,
					sql_fmt,
					"INFO FOR INDEX {} ON {} STRUCTURE",
					CoverStmts(i),
					CoverStmts(t)
				)
			}
		}
	}
}
