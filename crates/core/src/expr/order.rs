use std::ops::Deref;
use std::{cmp, fmt};

use crate::expr::Value;
use crate::expr::fmt::Fmt;
use crate::expr::idiom::Idiom;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct OrderList(pub Vec<Order>);

impl Deref for OrderList {
	type Target = Vec<Order>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for OrderList {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", Fmt::comma_separated(&self.0))
	}
}

impl OrderList {
	pub(crate) fn compare(&self, a: &Value, b: &Value) -> cmp::Ordering {
		for order in &self.0 {
			// Reverse the ordering if DESC
			let o = if order.direction {
				a.compare(b, &order.value.0, order.collate, order.numeric)
			} else {
				b.compare(a, &order.value.0, order.collate, order.numeric)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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
