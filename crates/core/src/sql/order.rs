use std::fmt;
use std::ops::Deref;

use crate::sql::Idiom;
use crate::sql::fmt::Fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
