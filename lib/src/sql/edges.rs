use crate::sql::comment::mightbespace;
use crate::sql::dir::{dir, Dir};
use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use crate::sql::table::{table, tables, Tables};
use crate::sql::thing::{thing, Thing};
use nom::branch::alt;
use nom::character::complete::char;
use nom::combinator::map;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Edges";

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Edges {
	pub dir: Dir,
	pub from: Thing,
	pub what: Tables,
}

impl fmt::Display for Edges {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.what.len() {
			0 => write!(f, "{}{}?", self.from, self.dir,),
			1 => write!(f, "{}{}{}", self.from, self.dir, self.what),
			_ => write!(f, "{}{}({})", self.from, self.dir, self.what),
		}
	}
}

impl Serialize for Edges {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			let mut val = serializer.serialize_struct(TOKEN, 3)?;
			val.serialize_field("dir", &self.dir)?;
			val.serialize_field("from", &self.from)?;
			val.serialize_field("what", &self.what)?;
			val.end()
		} else {
			serializer.serialize_none()
		}
	}
}

pub fn edges(i: &str) -> IResult<&str, Edges> {
	let (i, from) = thing(i)?;
	let (i, dir) = dir(i)?;
	let (i, what) = alt((simple, custom))(i)?;
	Ok((
		i,
		Edges {
			dir,
			from,
			what,
		},
	))
}

fn simple(i: &str) -> IResult<&str, Tables> {
	alt((any, one))(i)
}

fn custom(i: &str) -> IResult<&str, Tables> {
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, w) = alt((any, tables))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, w))
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
	fn edges_in() {
		let sql = "person:test<-likes";
		let res = edges(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:test<-likes", format!("{}", out));
	}

	#[test]
	fn edges_out() {
		let sql = "person:test->likes";
		let res = edges(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:test->likes", format!("{}", out));
	}

	#[test]
	fn edges_both() {
		let sql = "person:test<->likes";
		let res = edges(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:test<->likes", format!("{}", out));
	}

	#[test]
	fn edges_multiple() {
		let sql = "person:test->(likes, follows)";
		let res = edges(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:test->(likes, follows)", format!("{}", out));
	}
}
