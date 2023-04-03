use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::dir::{dir, Dir};
use crate::sql::error::IResult;
use crate::sql::field::Fields;
use crate::sql::group::Groups;
use crate::sql::idiom::{plain as idiom, Idiom};
use crate::sql::limit::Limit;
use crate::sql::order::Orders;
use crate::sql::split::Splits;
use crate::sql::start::Start;
use crate::sql::table::{table, tables, Tables};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::map;
use nom::combinator::opt;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Graph {
	pub dir: Dir,
	pub expr: Fields,
	pub what: Tables,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Orders>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl Graph {
	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl Display for Graph {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.0.len() <= 1 && self.cond.is_none() && self.alias.is_none() {
			Display::fmt(&self.dir, f)?;
			match self.what.len() {
				0 => f.write_char('?'),
				_ => Display::fmt(&self.what, f),
			}
		} else {
			write!(f, "{}(", self.dir)?;
			match self.what.len() {
				0 => f.write_char('?'),
				_ => Display::fmt(&self.what, f),
			}?;
			if let Some(ref v) = self.cond {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.split {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.group {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.order {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.limit {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.start {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.alias {
				write!(f, " AS {v}")?
			}
			f.write_char(')')
		}
	}
}

pub fn graph(i: &str) -> IResult<&str, Graph> {
	let (i, dir) = dir(i)?;
	let (i, (what, cond, alias)) = alt((simple, custom))(i)?;
	Ok((
		i,
		Graph {
			dir,
			expr: Fields::all(),
			what,
			cond,
			alias,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
		},
	))
}

fn simple(i: &str) -> IResult<&str, (Tables, Option<Cond>, Option<Idiom>)> {
	let (i, w) = alt((any, one))(i)?;
	Ok((i, (w, None, None)))
}

fn custom(i: &str) -> IResult<&str, (Tables, Option<Cond>, Option<Idiom>)> {
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, w) = alt((any, tables))(i)?;
	let (i, c) = opt(|i| {
		let (i, _) = shouldbespace(i)?;
		let (i, v) = cond(i)?;
		Ok((i, v))
	})(i)?;
	let (i, a) = opt(|i| {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("AS")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, v) = idiom(i)?;
		Ok((i, v))
	})(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, (w, c, a)))
}

fn one(i: &str) -> IResult<&str, Tables> {
	let (i, v) = table(i)?;
	Ok((i, Tables::from(v)))
}

fn any(i: &str) -> IResult<&str, Tables> {
	map(char('?'), |_| Tables::default())(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn graph_in() {
		let sql = "<-likes";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<-likes", format!("{}", out));
	}

	#[test]
	fn graph_out() {
		let sql = "->likes";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("->likes", format!("{}", out));
	}

	#[test]
	fn graph_both() {
		let sql = "<->likes";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<->likes", format!("{}", out));
	}

	#[test]
	fn graph_multiple() {
		let sql = "->(likes, follows)";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows)", format!("{}", out));
	}

	#[test]
	fn graph_aliases() {
		let sql = "->(likes, follows AS connections)";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows AS connections)", format!("{}", out));
	}

	#[test]
	fn graph_conditions() {
		let sql = "->(likes, follows WHERE influencer = true)";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows WHERE influencer = true)", format!("{}", out));
	}

	#[test]
	fn graph_conditions_aliases() {
		let sql = "->(likes, follows WHERE influencer = true AS connections)";
		let res = graph(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows WHERE influencer = true AS connections)", format!("{}", out));
	}
}
