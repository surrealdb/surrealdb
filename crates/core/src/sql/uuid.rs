use crate::sql::{escape::QuoteStr, strand::Strand};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;
use std::str::FromStr;

use super::Datetime;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Uuid(pub uuid::Uuid);

impl From<Uuid> for crate::expr::Uuid {
	fn from(v: Uuid) -> Self {
		crate::expr::Uuid(v.0)
	}
}

impl From<crate::expr::Uuid> for Uuid {
	fn from(v: crate::expr::Uuid) -> Self {
		Self(v.0)
	}
}

impl Deref for Uuid {
	type Target = uuid::Uuid;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Uuid {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "u{}", QuoteStr(&self.0.to_string()))
	}
}
