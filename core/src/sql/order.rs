use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Orders(pub Vec<Order>);

impl Deref for Orders {
	type Target = Vec<Order>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Orders {
	type Item = Order;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Orders {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ORDER BY {}", Fmt::comma_separated(&self.0))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Order {
	pub order: Idiom,
	pub random: bool,
	pub collate: bool,
	pub numeric: bool,
	/// true if the direction is ascending
	pub direction: bool,
}

impl Deref for Order {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.order
	}
}

impl fmt::Display for Order {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.order)?;
		if self.random {
			write!(f, "RAND()")?;
		}
		if self.collate {
			write!(f, " COLLATE")?;
		}
		if self.numeric {
			write!(f, " NUMERIC")?;
		}
		if !self.direction {
			write!(f, " DESC")?;
		}
		Ok(())
	}
}
