use std::time::Duration;

use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr as runtime;
use crate::expr::statements::define::{DefineKind, DefineSequenceStatement};
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct SequenceDefinition {
	pub name: Strand,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Duration>,
}

impl_kv_value_revisioned!(SequenceDefinition);

impl SequenceDefinition {
	fn to_runtime_definition(&self) -> DefineSequenceStatement {
		DefineSequenceStatement {
			kind: DefineKind::Default,
			name: runtime::Expr::Idiom(runtime::Idiom::field(self.name.clone())),
			batch: runtime::Expr::Literal(runtime::Literal::Integer(self.batch as i64)),
			start: runtime::Expr::Literal(runtime::Literal::Integer(self.start)),
			timeout: runtime::Expr::Literal(
				self.timeout
					.map(|d| runtime::Literal::Duration(d.into()))
					.unwrap_or(runtime::Literal::None),
			),
		}
	}
}

impl InfoStructure for SequenceDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
				"name" => self.name.into(),
				"batch" => Value::from(self.batch).structure(),
				"start" => Value::from(self.start).structure(),
				"timeout" => self.timeout.as_ref().map(|d| {
					Value::Duration((*d).into())
				}).unwrap_or(Value::None),
		})
	}
}

impl ToSql for &SequenceDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}
