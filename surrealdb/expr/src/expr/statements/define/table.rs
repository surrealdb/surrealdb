use surrealdb_strand::Strand;

use super::DefineKind;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::permission::Permissions;
use crate::expr::table_type::TableType;
use crate::expr::{Expr, Literal, View};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineTableStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Expr,
	pub table_type: TableType,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl Default for DefineTableStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::String(Strand::default())),
			drop: false,
			full: false,
			view: None,
			permissions: Permissions::default(),
			changefeed: None,
			comment: Expr::Literal(Literal::None),
			table_type: TableType::default(),
			graphql_alias: None,
			graphql_deprecated: None,
		}
	}
}
