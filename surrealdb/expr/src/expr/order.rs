use std::cmp;
use std::ops::Deref;

use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::Value;
use crate::expr::idiom::Idiom;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Ordering {
	Random,
	Order(OrderList),
}

impl Ordering {
	/// Whether evaluating the ordering can be done on a read-only transaction.
	pub fn read_only(&self) -> bool {
		match self {
			Ordering::Random => true,
			Ordering::Order(list) => list.read_only(),
		}
	}
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct OrderList(pub Vec<Order>);

impl OrderList {
	/// Whether evaluating every ordering key can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.value.read_only())
	}
}

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

impl OrderList {
	pub fn compare(&self, a: &Value, b: &Value) -> cmp::Ordering {
		for order in &self.0 {
			// Reverse the ordering if DESC
			let o = if order.direction {
				a.compare(b, &order.value.0, order.collate, order.numeric)
			} else {
				b.compare(a, &order.value.0, order.collate, order.numeric)
			};
			//
			match o {
				None | Some(cmp::Ordering::Equal) => continue,
				Some(cmp::Ordering::Greater) => return cmp::Ordering::Greater,
				Some(cmp::Ordering::Less) => return cmp::Ordering::Less,
			}
		}
		cmp::Ordering::Equal
	}
}

impl IntoIterator for OrderList {
	type Item = Order;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
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
