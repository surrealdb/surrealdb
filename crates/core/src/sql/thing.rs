use super::Table;
use super::id::range::IdRange;
use crate::sql::{Strand, escape::EscapeRid, id::Id};
use crate::syn;
use anyhow::Result;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Thing {
	/// Table name
	pub tb: String,
	pub id: Id,
}

impl From<Thing> for crate::expr::Thing {
	fn from(v: Thing) -> Self {
		crate::expr::Thing {
			tb: v.tb,
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::Thing> for Thing {
	fn from(v: crate::expr::Thing) -> Self {
		Thing {
			tb: v.tb,
			id: v.id.into(),
		}
	}
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.tb), self.id)
	}
}
