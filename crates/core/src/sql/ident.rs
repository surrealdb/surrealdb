use crate::sql::escape::EscapeIdent;
use std::fmt::{self};

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Ident(pub String);

impl From<crate::expr::Ident> for Ident {
	fn from(v: crate::expr::Ident) -> Self {
		Self(v.0)
	}
}

impl fmt::Display for Ident {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		EscapeIdent(&self.0).fmt(f)
	}
}
