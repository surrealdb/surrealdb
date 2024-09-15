use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Orders(pub Vec<Order>);

impl Orders {
	pub(crate) fn compare(&self, a: &Value, b: &Value) -> Ordering {
		for order in &self.0 {
			// Reverse the ordering if DESC
			let o = match order.random {
				true => {
					let a = rand::random::<f64>();
					let b = rand::random::<f64>();
					a.partial_cmp(&b)
				}
				false => match order.direction {
					true => a.compare(b, order, order.collate, order.numeric),
					false => b.compare(a, order, order.collate, order.numeric),
				},
			};
			//
			match o {
				Some(Ordering::Greater) => return Ordering::Greater,
				Some(Ordering::Equal) => continue,
				Some(Ordering::Less) => return Ordering::Less,
				None => continue,
			}
		}
		Ordering::Equal
	}
}

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
#[non_exhaustive]
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
		match self.direction {
			false => write!(f, " DESC")?,
			true => (),
		};
		Ok(())
	}
}
