use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::Ident;
use crate::val::Strand;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl From<DefineNamespaceStatement> for crate::expr::statements::DefineNamespaceStatement {
	fn from(v: DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: crate::expr::Expr::Idiom(crate::expr::Idiom::from(vec![
				crate::expr::Part::Field(v.name.into()),
			])),
			comment: v.comment.map(|s| crate::expr::Expr::Literal(crate::expr::Literal::Strand(s))),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineNamespaceStatement> for DefineNamespaceStatement {
	fn from(v: crate::expr::statements::DefineNamespaceStatement) -> Self {
		Self {
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
