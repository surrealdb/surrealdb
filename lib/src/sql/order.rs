use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::{basic, Idiom};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Orders(pub Vec<Order>);

impl Deref for Orders {
	type Target = Vec<Order>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Orders {
	type Item = Order;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Orders {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ORDER BY {}", Fmt::comma_separated(&self.0))
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Order {
	pub order: Idiom,
	pub random: bool,
	pub collate: bool,
	pub numeric: bool,
	pub direction: bool,
}

impl Deref for Order {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.order
	}
}

impl fmt::Display for Order {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.order)?;
		if self.random {
			write!(f, "RAND()")?;
		}
		if self.collate {
			write!(f, " COLLATE")?;
		}
		if self.numeric {
			write!(f, " NUMERIC")?;
		}
		match self.direction {
			false => write!(f, " DESC")?,
			true => (),
		};
		Ok(())
	}
}

pub fn order(i: &str) -> IResult<&str, Orders> {
	let (i, _) = tag_no_case("ORDER")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("BY"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((order_rand, separated_list1(commas, order_raw)))(i)?;
	Ok((i, Orders(v)))
}

fn order_rand(i: &str) -> IResult<&str, Vec<Order>> {
	let (i, _) = tag_no_case("RAND()")(i)?;
	Ok((
		i,
		vec![Order {
			order: Default::default(),
			random: true,
			collate: false,
			numeric: false,
			direction: true,
		}],
	))
}

fn order_raw(i: &str) -> IResult<&str, Order> {
	let (i, v) = basic(i)?;
	let (i, c) = opt(tuple((shouldbespace, tag_no_case("COLLATE"))))(i)?;
	let (i, n) = opt(tuple((shouldbespace, tag_no_case("NUMERIC"))))(i)?;
	let (i, d) = opt(alt((
		map(tuple((shouldbespace, tag_no_case("ASC"))), |_| true),
		map(tuple((shouldbespace, tag_no_case("DESC"))), |_| false),
	)))(i)?;
	Ok((
		i,
		Order {
			order: v,
			random: false,
			collate: c.is_some(),
			numeric: n.is_some(),
			direction: d.unwrap_or(true),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[test]
	fn order_statement() {
		let sql = "ORDER field";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field", format!("{}", out));
	}

	#[test]
	fn order_statement_by() {
		let sql = "ORDER BY field";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field", format!("{}", out));
	}

	#[test]
	fn order_statement_random() {
		let sql = "ORDER RAND()";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Default::default(),
				random: true,
				collate: false,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY RAND()", format!("{}", out));
	}

	#[test]
	fn order_statement_multiple() {
		let sql = "ORDER field, other.field";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![
				Order {
					order: Idiom::parse("field"),
					random: false,
					collate: false,
					numeric: false,
					direction: true,
				},
				Order {
					order: Idiom::parse("other.field"),
					random: false,
					collate: false,
					numeric: false,
					direction: true,
				},
			])
		);
		assert_eq!("ORDER BY field, other.field", format!("{}", out));
	}

	#[test]
	fn order_statement_collate() {
		let sql = "ORDER field COLLATE";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: true,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field COLLATE", format!("{}", out));
	}

	#[test]
	fn order_statement_numeric() {
		let sql = "ORDER field NUMERIC";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: true,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field NUMERIC", format!("{}", out));
	}

	#[test]
	fn order_statement_direction() {
		let sql = "ORDER field DESC";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: false,
				direction: false,
			}])
		);
		assert_eq!("ORDER BY field DESC", format!("{}", out));
	}

	#[test]
	fn order_statement_all() {
		let sql = "ORDER field COLLATE NUMERIC DESC";
		let res = order(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: true,
				numeric: true,
				direction: false,
			}])
		);
		assert_eq!("ORDER BY field COLLATE NUMERIC DESC", format!("{}", out));
	}
}
