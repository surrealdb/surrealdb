use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::dir::{dir, Dir};
use crate::sql::error::IResult;
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::table::{table, tables, Tables};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::map;
use nom::combinator::opt;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Graph {
	pub dir: Dir,
	pub what: Tables,
	pub cond: Option<Value>,
	pub alias: Option<Idiom>,
}

impl fmt::Display for Graph {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.what.0.len() <= 1 && self.cond.is_none() && self.alias.is_none() {
			write!(f, "{}", self.dir)?;
			match self.what.len() {
				0 => write!(f, "?"),
				_ => write!(f, "{}", self.what),
			}
		} else {
			write!(f, "{}(", self.dir)?;
			match self.what.len() {
				0 => write!(f, "?"),
				_ => write!(f, "{}", self.what),
			}?;
			if let Some(ref v) = self.cond {
				write!(f, " WHERE {}", v)?
			}
			if let Some(ref v) = self.alias {
				write!(f, " AS {}", v)?
			}
			write!(f, ")")
		}
	}
}

pub fn graph(i: &str) -> IResult<&str, Graph> {
	alt((graph_in, graph_out, graph_both))(i)
}

fn graph_in(i: &str) -> IResult<&str, Graph> {
	let (i, _) = char('<')(i)?;
	let (i, _) = char('-')(i)?;
	let (i, (what, cond, alias)) = alt((simple, custom))(i)?;
	Ok((
		i,
		Graph {
			dir: Dir::In,
			what,
			cond,
			alias,
		},
	))
}

fn graph_out(i: &str) -> IResult<&str, Graph> {
	let (i, _) = char('-')(i)?;
	let (i, _) = char('>')(i)?;
	let (i, (what, cond, alias)) = alt((simple, custom))(i)?;
	Ok((
		i,
		Graph {
			dir: Dir::Out,
			what,
			cond,
			alias,
		},
	))
}

fn graph_both(i: &str) -> IResult<&str, Graph> {
	let (i, _) = char('<')(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = char('>')(i)?;
	let (i, (what, cond, alias)) = alt((simple, custom))(i)?;
	Ok((
		i,
		Graph {
			dir: Dir::Both,
			what,
			cond,
			alias,
		},
	))
}

fn simple(i: &str) -> IResult<&str, (Tables, Option<Value>, Option<Idiom>)> {
	let (i, w) = alt((any, single))(i)?;
	Ok((i, (w, None, None)))
}

fn custom(i: &str) -> IResult<&str, (Tables, Option<Value>, Option<Idiom>)> {
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, w) = alt((any, multi))(i)?;
	let (i, c) = opt(cond)(i)?;
	let (i, a) = opt(alias)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, (w, c, a)))
}

fn single(i: &str) -> IResult<&str, Tables> {
	let (i, v) = table(i)?;
	Ok((i, Tables::from(v)))
}

fn multi(i: &str) -> IResult<&str, Tables> {
	let (i, v) = tables(i)?;
	Ok((i, Tables::from(v)))
}

fn any(i: &str) -> IResult<&str, Tables> {
	map(char('?'), |_| Tables::default())(i)
}

fn cond(i: &str) -> IResult<&str, Value> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("WHERE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, v))
}

fn alias(i: &str) -> IResult<&str, Idiom> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("AS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = idiom(i)?;
	Ok((i, v))
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
