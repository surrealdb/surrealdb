//! `sql` -> `expr` conversions for [`crate::sql::order`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::order::*;

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
