use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::permission::Permissions;
use crate::expr::table_type::TableType;
use crate::expr::{CoverStmts, Expr, Literal, View};

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
	/// Inline adjacency cache cap (`INLINE EDGES <n>`).
	pub inline_edges_cap: Option<u32>,
	/// Inline reference cache cap (`INLINE REFERENCES <n>`).
	pub inline_refs_cap: Option<u32>,
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
			inline_edges_cap: None,
			inline_refs_cap: None,
		}
	}
}

impl ToSql for DefineTableStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("DEFINE TABLE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
		f.push_str(" TYPE");
		match &self.table_type {
			TableType::Normal => f.push_str(" NORMAL"),
			TableType::Relation(rel) => {
				f.push_str(" RELATION");
				if !rel.from.is_empty() {
					f.push_str(" IN ");
					for (idx, k) in rel.from.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, sql_fmt, "{}", EscapeKwFreeIdent(k.as_str()));
					}
				}
				if !rel.to.is_empty() {
					f.push_str(" OUT ");
					for (idx, k) in rel.to.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, sql_fmt, "{}", EscapeKwFreeIdent(k.as_str()));
					}
				}
				if rel.enforced {
					f.push_str(" ENFORCED");
				}
				if rel.lightweight {
					f.push_str(" LIGHTWEIGHT");
				}
			}
			TableType::Any => f.push_str(" ANY"),
		}
		if self.drop {
			f.push_str(" DROP");
		}
		f.push_str(if self.full {
			" SCHEMAFULL"
		} else {
			" SCHEMALESS"
		});
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		if let Some(ref alias) = self.graphql_alias {
			write_sql!(f, sql_fmt, " GRAPHQL_ALIAS {}", common::fmt::QuoteStr(alias));
		}
		if let Some(ref reason) = self.graphql_deprecated {
			write_sql!(f, sql_fmt, " GRAPHQL_DEPRECATED {}", common::fmt::QuoteStr(reason));
		}
		if let Some(cap) = self.inline_edges_cap {
			write_sql!(f, sql_fmt, " INLINE EDGES {}", cap);
		}
		if let Some(cap) = self.inline_refs_cap {
			write_sql!(f, sql_fmt, " INLINE REFERENCES {}", cap);
		}
		if let Some(ref v) = self.view {
			write_sql!(f, sql_fmt, " {}", v);
		}
		if let Some(ref v) = self.changefeed {
			write_sql!(f, sql_fmt, " {}", v);
		}
		if sql_fmt.is_pretty() {
			f.push('\n');
			let inner_fmt = sql_fmt.increment();
			inner_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		write_sql!(f, sql_fmt, "{}", self.permissions);
	}
}
