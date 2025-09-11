use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Ident};
use crate::val::Strand;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub target_table: Ident,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Option<Strand>,
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(
			f,
			" {} ON {} WHEN {} THEN {}",
			self.name,
			self.target_table,
			self.when,
			Fmt::comma_separated(&self.then)
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl From<DefineEventStatement> for crate::expr::statements::DefineEventStatement {
	fn from(v: DefineEventStatement) -> Self {
		crate::expr::statements::DefineEventStatement {
			kind: v.kind.into(),
			name: crate::expr::Expr::Idiom(crate::expr::Idiom::from(vec![
				crate::expr::Part::Field(v.name.into()),
			])),
			target_table: crate::expr::Expr::Idiom(crate::expr::Idiom::from(vec![
				crate::expr::Part::Field(v.target_table.into()),
			])),
			when: v.when.into(),
			then: v.then.into_iter().map(From::from).collect(),
			comment: v.comment.map(|s| crate::expr::Expr::Literal(crate::expr::Literal::Strand(s))),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineEventStatement> for DefineEventStatement {
	fn from(v: crate::expr::statements::DefineEventStatement) -> Self {
		DefineEventStatement {
			kind: v.kind.into(),
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
			target_table: match v.target_table {
				crate::expr::Expr::Idiom(idiom) if idiom.len() == 1 => {
					if let Some(crate::expr::Part::Field(field)) = idiom.first() {
						field.clone().into()
					} else {
						crate::sql::Ident::new(String::new()).unwrap()
					}
				}
				_ => crate::sql::Ident::new(String::new()).unwrap(),
			},
			when: v.when.into(),
			then: v.then.into_iter().map(From::from).collect(),
			comment: v.comment.and_then(|expr| {
				if let crate::expr::Expr::Literal(crate::expr::Literal::Strand(s)) = expr {
					Some(s)
				} else {
					None
				}
			}),
		}
	}
}
