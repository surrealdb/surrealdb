use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::SqlValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::{cmp, fmt};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Ordering {
	Random,
	Order(OrderList),
}

impl From<Ordering> for crate::expr::order::Ordering {
	fn from(v: Ordering) -> Self {
		match v {
			Ordering::Random => Self::Random,
			Ordering::Order(list) => Self::Order(list.into()),
		}
	}
}

impl From<crate::expr::order::Ordering> for Ordering {
	fn from(v: crate::expr::order::Ordering) -> Self {
		match v {
			crate::expr::order::Ordering::Random => Self::Random,
			crate::expr::order::Ordering::Order(list) => Self::Order(list.into()),
		}
	}
}

crate::sql::impl_display_from_sql!(Ordering);

impl crate::sql::DisplaySql for Ordering {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl Deref for OrderList {
	type Target = Vec<Order>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<OrderList> for crate::expr::order::OrderList {
	fn from(v: OrderList) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::order::OrderList> for OrderList {
	fn from(v: crate::expr::order::OrderList) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

crate::sql::impl_display_from_sql!(OrderList);

impl crate::sql::DisplaySql for OrderList {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", Fmt::comma_separated(&self.0))
	}
}

impl OrderList {
	pub(crate) fn compare(&self, a: &SqlValue, b: &SqlValue) -> cmp::Ordering {
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

impl From<Order> for crate::expr::order::Order {
	fn from(v: Order) -> Self {
		Self {
			value: v.value.into(),
			collate: v.collate,
			numeric: v.numeric,
			direction: v.direction,
		}
	}
}
impl From<crate::expr::order::Order> for Order {
	fn from(v: crate::expr::order::Order) -> Self {
		Self {
			value: v.value.into(),
			collate: v.collate,
			numeric: v.numeric,
			direction: v.direction,
		}
	}
}

crate::sql::impl_display_from_sql!(Order);

impl crate::sql::DisplaySql for Order {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
