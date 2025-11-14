use std::ops::Deref;
use std::{cmp, fmt};

use crate::expr::Value;
use crate::expr::idiom::Idiom;
use crate::fmt::Fmt;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Ordering {
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
pub(crate) struct OrderList(pub(crate) Vec<Order>);

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
			let o = match order.direction {
				OrderDirection::Ascending => {
					a.compare(b, &order.value.0, order.collate, order.numeric)
				}
				OrderDirection::Descending => {
					b.compare(a, &order.value.0, order.collate, order.numeric)
				}
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

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum OrderDirection {
	#[default]
	Ascending,
	Descending,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Order {
	/// The value to order by
	pub(crate) value: Idiom,
	pub(crate) collate: bool,
	pub(crate) numeric: bool,
	pub(crate) direction: OrderDirection,
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
		if matches!(self.direction, OrderDirection::Descending) {
			write!(f, " DESC")?;
		}
		Ok(())
	}
}
