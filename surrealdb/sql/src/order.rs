use std::ops::Deref;

use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::Idiom;

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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OrderList(
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::atleast_one))]
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Order {
	/// The value to order by
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::basic_idiom))]
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
