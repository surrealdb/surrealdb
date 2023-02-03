use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::operator::{assigner, Operator};
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Data {
	EmptyExpression,
	SetExpression(Vec<(Idiom, Operator, Value)>),
	PatchExpression(Value),
	MergeExpression(Value),
	ReplaceExpression(Value),
	ContentExpression(Value),
	SingleExpression(Value),
	ValuesExpression(Vec<Vec<(Idiom, Value)>>),
	UpdateExpression(Vec<(Idiom, Operator, Value)>),
}

impl Default for Data {
	fn default() -> Self {
		Self::EmptyExpression
	}
}

impl Data {
	/// Fetch the 'id' field if one has been specified
	pub(crate) async fn rid(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		tb: &Table,
	) -> Result<Thing, Error> {
		match self {
			Self::MergeExpression(v) => {
				// This MERGE expression has an 'id' field
				v.compute(ctx, opt, txn, None).await?.rid().generate(tb, false)
			}
			Self::ReplaceExpression(v) => {
				// This REPLACE expression has an 'id' field
				v.compute(ctx, opt, txn, None).await?.rid().generate(tb, false)
			}
			Self::ContentExpression(v) => {
				// This CONTENT expression has an 'id' field
				v.compute(ctx, opt, txn, None).await?.rid().generate(tb, false)
			}
			Self::SetExpression(v) => match v.iter().find(|f| f.0.is_id()) {
				Some((_, _, v)) => {
					// This SET expression has an 'id' field
					v.compute(ctx, opt, txn, None).await?.generate(tb, false)
				}
				// This SET expression had no 'id' field
				_ => Ok(tb.generate()),
			},
			// Generate a random id for all other data clauses
			_ => Ok(tb.generate()),
		}
	}
}

impl Display for Data {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::EmptyExpression => Ok(()),
			Self::SetExpression(v) => write!(
				f,
				"SET {}",
				Fmt::comma_separated(
					v.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(f, "{l} {o} {r}",)))
				)
			),
			Self::PatchExpression(v) => write!(f, "PATCH {v}"),
			Self::MergeExpression(v) => write!(f, "MERGE {v}"),
			Self::ReplaceExpression(v) => write!(f, "REPLACE {v}"),
			Self::ContentExpression(v) => write!(f, "CONTENT {v}"),
			Self::SingleExpression(v) => Display::fmt(v, f),
			Self::ValuesExpression(v) => write!(
				f,
				"({}) VALUES {}",
				Fmt::comma_separated(v.first().unwrap().iter().map(|(v, _)| v)),
				Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"({})",
					Fmt::comma_separated(v.iter().map(|(_, v)| v))
				))))
			),
			Self::UpdateExpression(v) => write!(
				f,
				"ON DUPLICATE KEY UPDATE {}",
				Fmt::comma_separated(
					v.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(f, "{l} {o} {r}",)))
				)
			),
		}
	}
}

pub fn data(i: &str) -> IResult<&str, Data> {
	alt((set, patch, merge, replace, content))(i)
}

fn set(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("SET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, |i| {
		let (i, l) = idiom(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, o) = assigner(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, r) = value(i)?;
		Ok((i, (l, o, r)))
	})(i)?;
	Ok((i, Data::SetExpression(v)))
}

fn patch(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("PATCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, Data::PatchExpression(v)))
}

fn merge(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("MERGE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, Data::MergeExpression(v)))
}

fn replace(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("REPLACE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, Data::ReplaceExpression(v)))
}

fn content(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("CONTENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, Data::ContentExpression(v)))
}

pub fn single(i: &str) -> IResult<&str, Data> {
	let (i, v) = value(i)?;
	Ok((i, Data::SingleExpression(v)))
}

pub fn values(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("(")(i)?;
	let (i, fields) = separated_list1(commas, idiom)(i)?;
	let (i, _) = tag_no_case(")")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, values) = separated_list1(commas, |i| {
		let (i, _) = tag_no_case("(")(i)?;
		let (i, v) = separated_list1(commas, value)(i)?;
		let (i, _) = tag_no_case(")")(i)?;
		Ok((i, v))
	})(i)?;
	Ok((
		i,
		Data::ValuesExpression(
			values
				.into_iter()
				.map(|row| fields.iter().cloned().zip(row.into_iter()).collect())
				.collect(),
		),
	))
}

pub fn update(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("ON DUPLICATE KEY UPDATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, |i| {
		let (i, l) = idiom(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, o) = assigner(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, r) = value(i)?;
		Ok((i, (l, o, r)))
	})(i)?;
	Ok((i, Data::UpdateExpression(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn set_statement() {
		let sql = "SET field = true";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("SET field = true", format!("{}", out));
	}

	#[test]
	fn set_statement_multiple() {
		let sql = "SET field = true, other.field = false";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("SET field = true, other.field = false", format!("{}", out));
	}

	#[test]
	fn patch_statement() {
		let sql = "PATCH [{ field: true }]";
		let res = patch(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("PATCH [{ field: true }]", format!("{}", out));
	}

	#[test]
	fn merge_statement() {
		let sql = "MERGE { field: true }";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("MERGE { field: true }", format!("{}", out));
	}

	#[test]
	fn content_statement() {
		let sql = "CONTENT { field: true }";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CONTENT { field: true }", format!("{}", out));
	}

	#[test]
	fn replace_statement() {
		let sql = "REPLACE { field: true }";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("REPLACE { field: true }", format!("{}", out));
	}

	#[test]
	fn values_statement() {
		let sql = "(one, two, three) VALUES ($param, true, [1, 2, 3]), ($param, false, [4, 5, 6])";
		let res = values(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"(one, two, three) VALUES ($param, true, [1, 2, 3]), ($param, false, [4, 5, 6])",
			format!("{}", out)
		);
	}

	#[test]
	fn update_statement() {
		let sql = "ON DUPLICATE KEY UPDATE field = true, other.field = false";
		let res = update(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("ON DUPLICATE KEY UPDATE field = true, other.field = false", format!("{}", out));
	}
}
