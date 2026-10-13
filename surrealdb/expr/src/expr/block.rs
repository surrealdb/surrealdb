use std::ops::Deref;

use surrealdb_types::ToSql;

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Block(pub Vec<Expr>);

impl Deref for Block {
	type Target = [Expr];
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Block {
	/// Check if this block does only reads.
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl ToSql for Block {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let block: crate::sql::Block = self.clone().into();
		block.fmt_sql(f, fmt);
	}
}

impl InfoStructure for Block {
	fn structure(self) -> Value {
		Value::String(self.to_sql().into())
	}
}
