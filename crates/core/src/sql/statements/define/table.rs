use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Ident, Kind, Permissions, TableType, View};
use crate::val::Strand;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineTableStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Ident,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Strand>,
	pub table_type: TableType,
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
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
			write!(f, " COMMENT {comment}")?
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
			name: crate::expr::Expr::Idiom(crate::expr::Idiom::from(vec![
				crate::expr::Part::Field(v.name.into()),
			])),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.map(|s| crate::expr::Expr::Literal(crate::expr::Literal::Strand(s))),
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
			name: match v.name {
				crate::expr::Expr::Idiom(idiom) if idiom.len() == 1 => {
					if let Some(crate::expr::Part::Field(field)) = idiom.first() {
						field.clone().into()
					} else {
						crate::sql::Ident::new(String::new()).unwrap()
					}
				}
				_ => crate::sql::Ident::new(String::new()).unwrap(),
			},
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.and_then(|expr| {
				if let crate::expr::Expr::Literal(crate::expr::Literal::Strand(s)) = expr {
					Some(s)
				} else {
					None
				}
			}),
			table_type: v.table_type.into(),
		}
	}
}
