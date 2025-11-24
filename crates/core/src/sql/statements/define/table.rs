use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::fmt::{CoverStmts, is_pretty, pretty_indent};
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

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", CoverStmts(&self.name))?;
		write!(f, " TYPE")?;
		match &self.table_type {
			TableType::Normal => {
				f.write_str(" NORMAL")?;
			}
			TableType::Relation(rel) => {
				f.write_str(" RELATION")?;
				if let Some(Kind::Record(kind)) = &rel.from {
					write!(f, " IN ",)?;
					for (idx, k) in kind.iter().enumerate() {
						if idx != 0 {
							write!(f, " | ")?;
						}
						write!(f, "{}", k)?;
					}
				}

				if let Some(Kind::Record(kind)) = &rel.to {
					write!(f, " OUT ",)?;
					for (idx, k) in kind.iter().enumerate() {
						if idx != 0 {
							write!(f, " | ")?;
						}
						write!(f, "{}", k)?;
					}
				}
				if rel.enforced {
					write!(f, " ENFORCED")?;
				}
			}
			TableType::Any => {
				f.write_str(" ANY")?;
			}
		}
		if self.drop {
			f.write_str(" DROP")?;
		}
		f.write_str(if self.full {
			" SCHEMAFULL"
		} else {
			" SCHEMALESS"
		})?;
		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {}", CoverStmts(comment))?
		}
		if let Some(ref v) = self.view {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "{}", self.permissions)?;
		Ok(())
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
