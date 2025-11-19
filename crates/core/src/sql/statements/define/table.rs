use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::{Expr, Kind, Literal, Permissions, TableType, View};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineTableStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Expr>,
	pub table_type: TableType,
}

impl Default for DefineTableStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			drop: false,
			full: false,
			view: None,
			permissions: Permissions::none(),
			changefeed: None,
			comment: None,
			table_type: TableType::default(),
		}
	}
}

impl ToSql for DefineTableStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("DEFINE TABLE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		write_sql!(f, " {}", self.name);
		f.push_str(" TYPE");
		match &self.table_type {
			TableType::Normal => f.push_str(" NORMAL"),
			TableType::Relation(rel) => {
				f.push_str(" RELATION");
				if let Some(Kind::Record(kind)) = &rel.from {
					f.push_str(" IN ");
					for (idx, k) in kind.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, "{}", k);
					}
				}
				if let Some(Kind::Record(kind)) = &rel.to {
					f.push_str(" OUT ");
					for (idx, k) in kind.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, "{}", k);
					}
				}
				if rel.enforced {
					f.push_str(" ENFORCED");
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
		if let Some(ref comment) = self.comment {
			f.push_str(" COMMENT ");
			comment.fmt_sql(f, fmt);
		}
		if let Some(ref v) = self.view {
			f.push(' ');
			v.fmt_sql(f, fmt);
		}
		if let Some(ref v) = self.changefeed {
			f.push(' ');
			v.fmt_sql(f, fmt);
		}
		if fmt.is_pretty() {
			f.push('\n');
			let inner_fmt = fmt.increment();
			inner_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		self.permissions.fmt_sql(f, fmt);
	}
}

impl From<DefineTableStatement> for crate::expr::statements::DefineTableStatement {
	fn from(v: DefineTableStatement) -> Self {
		crate::expr::statements::DefineTableStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.map(|x| x.into()),
			table_type: v.table_type.into(),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineTableStatement> for DefineTableStatement {
	fn from(v: crate::expr::statements::DefineTableStatement) -> Self {
		DefineTableStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.map(|x| x.into()),
			table_type: v.table_type.into(),
		}
	}
}
