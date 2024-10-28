use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{cmp, fmt};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Ordering {
	Random,
	Order(OrderList),
}

impl fmt::Display for Ordering {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Ordering::Random => write!(f, "ORDER BY RAND()"),
			Ordering::Order(list) => writeln!(f, "ORDER BY {list}"),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OrderList(pub Vec<Order>);

impl fmt::Display for OrderList {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", Fmt::comma_separated(&self.0))
	}
}

impl OrderList {
	pub(crate) fn compare(&self, a: &Value, b: &Value) -> cmp::Ordering {
		for order in &self.0 {
			// Reverse the ordering if DESC
			let o = match order.direction {
				true => a.compare(b, &order.value.0, order.collate, order.numeric),
				false => b.compare(a, &order.value.0, order.collate, order.numeric),
			};
			//
			match o {
				Some(cmp::Ordering::Greater) => return cmp::Ordering::Greater,
				Some(cmp::Ordering::Equal) => continue,
				Some(cmp::Ordering::Less) => return cmp::Ordering::Less,
				None => continue,
			}
		}
		cmp::Ordering::Equal
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Order {
	/// The value to order by
	pub value: Idiom,
	pub collate: bool,
	pub numeric: bool,
	/// true if the direction is ascending
	pub direction: bool,
}

impl fmt::Display for Order {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.value)?;
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OldOrders(pub Vec<OldOrder>);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OldOrder {
	pub order: Idiom,
	pub random: bool,
	pub collate: bool,
	pub numeric: bool,
	/// true if the direction is ascending
	pub direction: bool,
}
