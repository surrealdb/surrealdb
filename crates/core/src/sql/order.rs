use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::Fmt;
use crate::sql::Idiom;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Ordering {
	Random,
	Order(OrderList),
}

impl surrealdb_types::ToSql for Ordering {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		match self {
			Ordering::Random => f.push_str("ORDER BY RAND()"),
			Ordering::Order(list) => {
				write_sql!(f, fmt, "ORDER BY {}", list);
			}
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
pub struct OrderList(
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub  Vec<Order>,
);

impl Deref for OrderList {
	type Target = Vec<Order>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl ToSql for OrderList {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}", Fmt::comma_separated(&self.0))
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

impl ToSql for Order {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.value.fmt_sql(f, fmt);
		if self.collate {
			f.push_str(" COLLATE");
		}
		if self.numeric {
			f.push_str(" NUMERIC");
		}
		if !self.direction {
			f.push_str(" DESC");
		}
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
