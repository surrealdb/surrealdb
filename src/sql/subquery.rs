use crate::ctx::Context;
use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::statements::create::{create, CreateStatement};
use crate::sql::statements::delete::{delete, DeleteStatement};
use crate::sql::statements::ifelse::{ifelse, IfelseStatement};
use crate::sql::statements::insert::{insert, InsertStatement};
use crate::sql::statements::relate::{relate, RelateStatement};
use crate::sql::statements::select::{select, SelectStatement};
use crate::sql::statements::update::{update, UpdateStatement};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Subquery {
	Value(Value),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
	Ifelse(IfelseStatement),
}

impl PartialOrd for Subquery {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		unreachable!()
	}
}

impl fmt::Display for Subquery {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Subquery::Value(v) => write!(f, "({})", v),
			Subquery::Select(v) => write!(f, "({})", v),
			Subquery::Create(v) => write!(f, "({})", v),
			Subquery::Update(v) => write!(f, "({})", v),
			Subquery::Delete(v) => write!(f, "({})", v),
			Subquery::Relate(v) => write!(f, "({})", v),
			Subquery::Insert(v) => write!(f, "({})", v),
			Subquery::Ifelse(v) => write!(f, "{}", v),
		}
	}
}

impl dbs::Process for Subquery {
	fn process(
		&self,
		ctx: &Runtime,
		opt: &Options,
		exe: &mut Executor,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self {
			Subquery::Value(ref v) => v.process(ctx, opt, exe, doc),
			Subquery::Ifelse(ref v) => v.process(ctx, opt, exe, doc),
			Subquery::Select(ref v) => {
				// Duplicate options
				let opt = opt.dive()?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value(String::from("parent"), doc);
				}
				// Prepare context
				let ctx = ctx.freeze();
				// Process subquery
				let res = v.process(&ctx, &opt, exe, doc)?;
				// Process result
				match v.limit() {
					1 => match v.expr.single() {
						Some(v) => res.first(&ctx, &opt, exe).get(&ctx, &opt, exe, &v).ok(),
						None => res.first(&ctx, &opt, exe).ok(),
					},
					_ => match v.expr.single() {
						Some(v) => res.get(&ctx, &opt, exe, &v).ok(),
						None => res.ok(),
					},
				}
			}
			Subquery::Create(ref v) => {
				// Duplicate options
				let opt = opt.dive()?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value(String::from("parent"), doc);
				}
				// Prepare context
				let ctx = ctx.freeze();
				// Process subquery
				match v.process(&ctx, &opt, exe, doc)? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.value.remove(0)),
						_ => Ok(v.into()),
					},
					v => Ok(v),
				}
			}
			Subquery::Update(ref v) => {
				// Duplicate options
				let opt = opt.dive()?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value(String::from("parent"), doc);
				}
				// Prepare context
				let ctx = ctx.freeze();
				// Process subquery
				match v.process(&ctx, &opt, exe, doc)? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.value.remove(0)),
						_ => Ok(v.into()),
					},
					v => Ok(v),
				}
			}
			Subquery::Delete(ref v) => {
				// Duplicate options
				let opt = opt.dive()?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value(String::from("parent"), doc);
				}
				// Prepare context
				let ctx = ctx.freeze();
				// Process subquery
				match v.process(&ctx, &opt, exe, doc)? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.value.remove(0)),
						_ => Ok(v.into()),
					},
					v => Ok(v),
				}
			}
			Subquery::Relate(ref v) => {
				// Duplicate options
				let opt = opt.dive()?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value(String::from("parent"), doc);
				}
				// Prepare context
				let ctx = ctx.freeze();
				// Process subquery
				match v.process(&ctx, &opt, exe, doc)? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.value.remove(0)),
						_ => Ok(v.into()),
					},
					v => Ok(v),
				}
			}
			Subquery::Insert(ref v) => {
				// Duplicate options
				let opt = opt.dive()?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value(String::from("parent"), doc);
				}
				// Prepare context
				let ctx = ctx.freeze();
				// Process subquery
				match v.process(&ctx, &opt, exe, doc)? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.value.remove(0)),
						_ => Ok(v.into()),
					},
					v => Ok(v),
				}
			}
		}
	}
}

pub fn subquery(i: &str) -> IResult<&str, Subquery> {
	alt((subquery_ifelse, subquery_others))(i)
}

fn subquery_ifelse(i: &str) -> IResult<&str, Subquery> {
	let (i, v) = map(ifelse, |v| Subquery::Ifelse(v))(i)?;
	Ok((i, v))
}

fn subquery_others(i: &str) -> IResult<&str, Subquery> {
	let (i, _) = tag("(")(i)?;
	let (i, v) = alt((
		map(select, |v| Subquery::Select(v)),
		map(create, |v| Subquery::Create(v)),
		map(update, |v| Subquery::Update(v)),
		map(delete, |v| Subquery::Delete(v)),
		map(relate, |v| Subquery::Relate(v)),
		map(insert, |v| Subquery::Insert(v)),
		map(value, |v| Subquery::Value(v)),
	))(i)?;
	let (i, _) = tag(")")(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn subquery_expression_statement() {
		let sql = "(1 + 2 + 3)";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(1 + 2 + 3)", format!("{}", out))
	}

	#[test]
	fn subquery_ifelse_statement() {
		let sql = "IF true THEN false END";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("IF true THEN false END", format!("{}", out))
	}

	#[test]
	fn subquery_select_statement() {
		let sql = "(SELECT * FROM test)";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(SELECT * FROM test)", format!("{}", out))
	}
}
