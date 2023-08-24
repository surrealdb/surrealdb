use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::{fmt_separated_by, Fmt};
use crate::sql::part::{basic_part, first, graph, local_part, part, Part};
use crate::sql::part::{flatten, Next};
use crate::sql::paths::{ID, IN, META, OUT};
use crate::sql::value::Value;
use md5::Digest;
use md5::Md5;
use nom::branch::alt;
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::multi::{many0, many1};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Idiom";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub struct Idioms(pub Vec<Idiom>);

impl Deref for Idioms {
	type Target = Vec<Idiom>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Idioms {
	type Item = Idiom;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
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
#[serde(rename = "$surrealdb::private::sql::Idiom")]
#[revisioned(revision = 1)]
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

impl From<&[Part]> for Idiom {
	fn from(v: &[Part]) -> Self {
		Self(v.to_vec())
	}
}

impl Idiom {
	/// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
	}
	/// Convert this Idiom to a unique hash
	pub(crate) fn to_hash(&self) -> String {
		let mut hasher = Md5::new();
		hasher.update(self.to_string().as_str());
		format!("{:x}", hasher.finalize())
	}
	/// Convert this Idiom to a JSON Path string
	pub(crate) fn to_path(&self) -> String {
		format!("/{self}").replace(']', "").replace(&['.', '['][..], "/")
	}
	/// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		self.0
			.iter()
			.cloned()
			.filter(|p| {
				matches!(p, Part::Field(_) | Part::Start(_) | Part::Value(_) | Part::Graph(_))
			})
			.collect::<Vec<_>>()
			.into()
	}
	/// Check if this expression is an 'id' field
	pub(crate) fn is_id(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&ID[0])
	}
	/// Check if this expression is an 'in' field
	pub(crate) fn is_in(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&IN[0])
	}
	/// Check if this expression is an 'out' field
	pub(crate) fn is_out(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&OUT[0])
	}
	/// Check if this expression is an 'out' field
	pub(crate) fn is_meta(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&META[0])
	}
	/// Check if this is an expression with multiple yields
	pub(crate) fn is_multi_yield(&self) -> bool {
		self.iter().any(Self::split_multi_yield)
	}
	/// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn split_multi_yield(v: &Part) -> bool {
		matches!(v, Part::Graph(g) if g.alias.is_some())
	}
}

impl Idiom {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.0.iter().any(|v| v.writeable())
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self.first() {
			// The starting part is a value
			Some(Part::Start(v)) => {
				v.compute(ctx, opt, txn, doc)
					.await?
					.get(ctx, opt, txn, doc, self.as_ref().next())
					.await?
					.compute(ctx, opt, txn, doc)
					.await
			}
			// Otherwise use the current document
			_ => match doc {
				// There is a current document
				Some(v) => {
					v.doc.get(ctx, opt, txn, doc, self).await?.compute(ctx, opt, txn, doc).await
				}
				// There isn't any document
				None => Ok(Value::None),
			},
		}
	}
}

impl Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&Fmt::new(
				self.0.iter().enumerate().map(|args| {
					Fmt::new(args, |(i, p), f| match (i, p) {
						(0, Part::Field(v)) => Display::fmt(v, f),
						_ => Display::fmt(p, f),
					})
				}),
				fmt_separated_by(""),
			),
			f,
		)
	}
}

/// Used in DEFINE FIELD and DEFINE INDEX clauses
pub fn local(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(local_part)(i)?;
	// Flatten is only allowed at the end
	let (i, flat) = opt(flatten)(i)?;
	if let Some(p) = flat {
		v.push(p);
	}
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

/// Used in a SPLIT, ORDER, and GROUP clauses
pub fn basic(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(basic_part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

/// A simple idiom with one or more parts
pub fn plain(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = alt((first, graph))(i)?;
	let (i, mut v) = many0(part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

/// Reparse a value which might part of an idiom.
pub fn reparse_idiom_start(start: Value, i: &str) -> IResult<&str, Value> {
	if start.can_start_idiom() {
		if let (i, Some(mut parts)) = opt(many1(part))(i)? {
			let start = Part::Start(start);
			parts.insert(0, start);
			let v = Value::from(Idiom::from(parts));
			return Ok((i, v));
		}
	}
	Ok((i, start))
}

/// A complex idiom with graph or many parts excluding idioms which start with a value.
pub fn multi_without_start(i: &str) -> IResult<&str, Idiom> {
	alt((
		|i| {
			let (i, p) = graph(i)?;
			let (i, mut v) = many0(part)(i)?;
			v.insert(0, p);
			Ok((i, Idiom::from(v)))
		},
		|i| {
			let (i, p) = first(i)?;
			let (i, mut v) = many1(part)(i)?;
			v.insert(0, p);
			Ok((i, Idiom::from(v)))
		},
	))(i)
}

/// A simple field based idiom
pub fn path(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

/// A full complex idiom with any number of parts
#[cfg(test)]
pub fn idiom(i: &str) -> IResult<&str, Idiom> {
	use nom::combinator::fail;

	use crate::sql::value::value;

	alt((
		plain,
		alt((multi_without_start, |i| {
			let (i, v) = value(i)?;
			let (i, v) = reparse_idiom_start(v, i)?;
			if let Value::Idiom(x) = v {
				return Ok((i, x));
			}
			fail(i)
		})),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::dir::Dir;
	use crate::sql::expression::Expression;
	use crate::sql::field::Fields;
	use crate::sql::graph::Graph;
	use crate::sql::number::Number;
	use crate::sql::param::Param;
	use crate::sql::table::Table;
	use crate::sql::test::Parse;
	use crate::sql::thing::Thing;

	#[test]
	fn idiom_number() {
		let sql = "13.495";
		let res = idiom(sql);
		assert!(res.is_err());
	}

	#[test]
	fn idiom_normal() {
		let sql = "test";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_quoted_backtick() {
		let sql = "`test`";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_nested() {
		let sql = "test.temp";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp")]));
	}

	#[test]
	fn idiom_nested_quoted() {
		let sql = "test.`some key`";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.`some key`", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("some key")]));
	}

	#[test]
	fn idiom_nested_array_all() {
		let sql = "test.temp[*]";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[*]", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"), Part::All]));
	}

	#[test]
	fn idiom_nested_array_last() {
		let sql = "test.temp[$]";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[$]", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"), Part::Last]));
	}

	#[test]
	fn idiom_nested_array_value() {
		let sql = "test.temp[*].text";
		let res = idiom(sql);
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
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::Where(Value::from(Expression::parse("test = true"))),
				Part::from("text")
			])
		);
	}

	#[test]
	fn idiom_nested_array_condition() {
		let sql = "test.temp[WHERE test = true].text";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::Where(Value::from(Expression::parse("test = true"))),
				Part::from("text")
			])
		);
	}

	#[test]
	fn idiom_start_param_local_field() {
		let sql = "$test.temporary[0].embedded…";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("$test.temporary[0].embedded…", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Param::from("test").into()),
				Part::from("temporary"),
				Part::Index(Number::Int(0)),
				Part::from("embedded"),
				Part::Flatten,
			])
		);
	}

	#[test]
	fn idiom_start_thing_remote_traversal() {
		let sql = "person:test.friend->like->person";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("person:test.friend->like->person", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Thing::from(("person", "test")).into()),
				Part::from("friend"),
				Part::Graph(Graph {
					dir: Dir::Out,
					expr: Fields::all(),
					what: Table::from("like").into(),
					cond: None,
					alias: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
				}),
				Part::Graph(Graph {
					dir: Dir::Out,
					expr: Fields::all(),
					what: Table::from("person").into(),
					cond: None,
					alias: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
				}),
			])
		);
	}
}
