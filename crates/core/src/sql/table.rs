use crate::sql::{Id, Ident, Thing, escape::EscapeIdent, fmt::Fmt, strand::no_nul_bytes};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Tables(pub Vec<Table>);

impl From<Table> for Tables {
	fn from(v: Table) -> Self {
		Tables(vec![v])
	}
}

impl Deref for Tables {
	type Target = Vec<Table>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Tables {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

impl From<Tables> for crate::expr::Tables {
	fn from(v: Tables) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Tables> for Tables {
	fn from(v: crate::expr::Tables) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Ord)]
pub struct Table(pub String);

impl From<Table> for crate::expr::Table {
	fn from(v: Table) -> Self {
		crate::expr::Table(v.0)
	}
}

impl From<crate::expr::Table> for Table {
	fn from(v: crate::expr::Table) -> Self {
		Self(v.0)
	}
}
