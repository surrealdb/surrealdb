use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::part::Next;
use crate::sql::part::{all, field, first, graph, index, last, part, thing, Part};
use crate::sql::paths::{ID, IN, OUT};
use crate::sql::value::Value;
use md5::Digest;
use md5::Md5;
use nom::branch::alt;
use nom::multi::separated_list1;
use nom::multi::{many0, many1};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Idioms(pub Vec<Idiom>);

impl Deref for Idioms {
	type Target = Vec<Idiom>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Idioms {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

pub fn locals(i: &str) -> IResult<&str, Idioms> {
	let (i, v) = separated_list1(commas, local)(i)?;
	Ok((i, Idioms(v)))
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Idiom(pub Vec<Part>);

impl Deref for Idiom {
	type Target = [Part];
	fn deref(&self) -> &Self::Target {
		self.0.as_slice()
	}
}

impl From<String> for Idiom {
	fn from(v: String) -> Self {
		Self(vec![Part::from(v)])
	}
}

impl From<Vec<Part>> for Idiom {
	fn from(v: Vec<Part>) -> Self {
		Self(v)
	}
}

impl Idiom {
	// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
	}
	// Convert this Idiom to a unique hash
	pub(crate) fn to_hash(&self) -> String {
		let mut hasher = Md5::new();
		hasher.update(self.to_string().as_str());
		format!("{:x}", hasher.finalize())
	}
	// Convert this Idiom to a JSON Path string
	pub(crate) fn to_path(&self) -> String {
		format!("/{}", self).replace(']', "").replace(&['.', '['][..], "/")
	}
	// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		self.0
			.iter()
			.cloned()
			.filter(|p| matches!(p, Part::Field(_) | Part::Thing(_) | Part::Graph(_)))
			.collect::<Vec<_>>()
			.into()
	}
	// Check if this expression is an 'id' field
	pub(crate) fn is_id(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&ID[0])
	}
	// Check if this expression is an 'in' field
	pub(crate) fn is_in(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&IN[0])
	}
	// Check if this expression is an 'out' field
	pub(crate) fn is_out(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&OUT[0])
	}
	// Check if this is an expression with multiple yields
	pub(crate) fn is_multi_yield(&self) -> bool {
		self.iter().any(Self::split_multi_yield)
	}
	// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn split_multi_yield(v: &Part) -> bool {
		matches!(v, Part::Graph(g) if g.alias.is_some())
	}
}

impl Idiom {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self.first() {
			// The first part is a thing record
			Some(Part::Thing(v)) => {
				// Use the thing as the document
				let v: Value = v.clone().into();
				// Fetch the Idiom from the document
				v.get(ctx, opt, txn, self.as_ref().next())
					.await?
					.compute(ctx, opt, txn, Some(&v))
					.await
			}
			// Otherwise use the current document
			_ => match doc {
				// There is a current document
				Some(v) => v.get(ctx, opt, txn, self).await?.compute(ctx, opt, txn, doc).await,
				// There isn't any document
				None => Ok(Value::None),
			},
		}
	}
}

impl fmt::Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{}",
			self.0
				.iter()
				.enumerate()
				.map(|(i, p)| match (i, p) {
					(0, Part::Field(v)) => format!("{}", v),
					_ => format!("{}", p),
				})
				.collect::<Vec<_>>()
				.join("")
		)
	}
}

// Used in a DEFINE FIELD and DEFINE INDEX clauses
pub fn local(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(alt((all, index, field)))(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

// Used in a SPLIT, ORDER, and GROUP clauses
pub fn basic(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(alt((all, last, index, field)))(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

// Used in a $param definition
pub fn param(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

pub fn idiom(i: &str) -> IResult<&str, Idiom> {
	alt((
		|i| {
			let (i, p) = alt((thing, graph))(i)?;
			let (i, mut v) = many1(part)(i)?;
			v.insert(0, p);
			Ok((i, Idiom::from(v)))
		},
		|i| {
			let (i, p) = alt((first, graph))(i)?;
			let (i, mut v) = many0(part)(i)?;
			v.insert(0, p);
			Ok((i, Idiom::from(v)))
		},
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::dir::Dir;
	use crate::sql::expression::Expression;
	use crate::sql::graph::Graph;
	use crate::sql::table::Table;
	use crate::sql::test::Parse;
	use crate::sql::thing::Thing;

	#[test]
	fn idiom_normal() {
		let sql = "test";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_quoted_backtick() {
		let sql = "`test`";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_nested() {
		let sql = "test.temp";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"),]));
	}

	#[test]
	fn idiom_nested_quoted() {
		let sql = "test.`some key`";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.`some key`", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("some key"),]));
	}

	#[test]
	fn idiom_nested_array_all() {
		let sql = "test.temp[*]";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[*]", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"), Part::All,]));
	}

	#[test]
	fn idiom_nested_array_last() {
		let sql = "test.temp[$]";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[$]", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"), Part::Last,]));
	}

	#[test]
	fn idiom_nested_array_value() {
		let sql = "test.temp[*].text";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[*].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![Part::from("test"), Part::from("temp"), Part::All, Part::from("text")])
		);
	}

	#[test]
	fn idiom_nested_array_question() {
		let sql = "test.temp[? test = true].text";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::from(Value::from(Expression::parse("test = true"))),
				Part::from("text")
			])
		);
	}

	#[test]
	fn idiom_nested_array_condition() {
		let sql = "test.temp[WHERE test = true].text";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::from(Value::from(Expression::parse("test = true"))),
				Part::from("text")
			])
		);
	}

	#[test]
	fn idiom_start_thing_remote_traversal() {
		let sql = "person:test.friend->like->person";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:test.friend->like->person", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from(Thing::from(("person", "test"))),
				Part::from("friend"),
				Part::from(Graph {
					dir: Dir::Out,
					what: Table::from("like").into(),
					cond: None,
					alias: None,
				}),
				Part::from(Graph {
					dir: Dir::Out,
					what: Table::from("person").into(),
					cond: None,
					alias: None,
				}),
			])
		);
	}
}
