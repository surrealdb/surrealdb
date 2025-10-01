use std::time::Duration;

use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::ToSql;
use crate::sql::statements::define::{DefineKind, DefineSequenceStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct SequenceDefinition {
	pub name: String,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Duration>,
}

impl_kv_value_revisioned!(SequenceDefinition);

impl SequenceDefinition {
	fn to_sql_definition(&self) -> DefineSequenceStatement {
		DefineSequenceStatement {
			kind: DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			batch: crate::sql::Expr::Literal(crate::sql::Literal::Integer(self.batch as i64)),
			start: crate::sql::Expr::Literal(crate::sql::Literal::Integer(self.start)),
			timeout: self.timeout.map(|t| t.into()),
		}
	}
}

impl InfoStructure for SequenceDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
				"name".to_string() => self.name.into(),
				"batch".to_string() => Value::from(self.batch).structure(),
				"start".to_string() => Value::from(self.start).structure(),
				"timeout".to_string() => self.timeout.as_ref().map(|d| {
					Value::Duration((*d).into())
				}).unwrap_or(Value::None),
		})
	}
}

impl ToSql for &SequenceDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
