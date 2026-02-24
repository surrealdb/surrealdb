use std::cmp;
use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Value;
use crate::expr::idiom::Idiom;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Ordering {
	Random,
	Order(OrderList),
}

impl ToSql for Ordering {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let sql_ordering: crate::sql::order::Ordering = self.clone().into();
		sql_ordering.fmt_sql(f, sql_fmt);
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

impl ToSql for OrderList {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let sql_order_list: crate::sql::order::OrderList = self.clone().into();
		sql_order_list.fmt_sql(f, sql_fmt);
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
pub(crate) struct Order {
	/// The value to order by
	pub(crate) value: Idiom,
	pub(crate) collate: bool,
	pub(crate) numeric: bool,
	/// true if the direction is ascending
	pub(crate) direction: bool,
}

impl ToSql for Order {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let sql_order: crate::sql::Order = self.clone().into();
		sql_order.fmt_sql(f, sql_fmt);
	}
}
