use crate::sql::{escape::EscapeIdent, strand::no_nul_bytes};
use revision::revisioned;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Ident(#[serde(with = "no_nul_bytes")] pub String);

impl From<crate::expr::Ident> for Ident {
	fn from(v: crate::expr::Ident) -> Self {
		Self(v.0)
	}
}
