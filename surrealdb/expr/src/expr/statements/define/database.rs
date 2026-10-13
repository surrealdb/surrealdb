use super::DefineKind;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Literal};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub strict: bool,
	pub comment: Expr,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
			changefeed: None,
			strict: false,
		}
	}
}
impl InfoStructure for DefineDatabaseStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.structure(),
			"comment" => self.comment.structure(),
		})
	}
}
