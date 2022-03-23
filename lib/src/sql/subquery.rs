use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::statements::create::{create, CreateStatement};
use crate::sql::statements::delete::{delete, DeleteStatement};
use crate::sql::statements::ifelse::{ifelse, IfelseStatement};
use crate::sql::statements::insert::{insert, InsertStatement};
use crate::sql::statements::relate::{relate, RelateStatement};
use crate::sql::statements::select::{select, SelectStatement};
use crate::sql::statements::update::{update, UpdateStatement};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::character::complete::char;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Subquery {
	Value(Value),
	Ifelse(IfelseStatement),
	Select(Arc<SelectStatement>),
	Create(Arc<CreateStatement>),
	Update(Arc<UpdateStatement>),
	Delete(Arc<DeleteStatement>),
	Relate(Arc<RelateStatement>),
	Insert(Arc<InsertStatement>),
}

impl PartialOrd for Subquery {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Subquery {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self {
			Subquery::Value(ref v) => v.compute(ctx, opt, txn, doc).await,
			Subquery::Ifelse(ref v) => v.compute(ctx, opt, txn, doc).await,
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
				let res = Arc::clone(v).compute(&ctx, &opt, txn, doc).await?;
				// Process result
				match v.limit() {
					1 => match v.expr.single() {
						Some(v) => res.first(&ctx, &opt, txn).await?.get(&ctx, &opt, txn, &v).await,
						None => res.first(&ctx, &opt, txn).await,
					},
					_ => match v.expr.single() {
						Some(v) => res.get(&ctx, &opt, txn, &v).await,
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
				match Arc::clone(v).compute(&ctx, &opt, txn, doc).await? {
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
				match Arc::clone(v).compute(&ctx, &opt, txn, doc).await? {
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
				match Arc::clone(v).compute(&ctx, &opt, txn, doc).await? {
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
				match Arc::clone(v).compute(&ctx, &opt, txn, doc).await? {
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
				match Arc::clone(v).compute(&ctx, &opt, txn, doc).await? {
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

pub fn subquery(i: &str) -> IResult<&str, Subquery> {
	alt((subquery_ifelse, subquery_others))(i)
}

fn subquery_ifelse(i: &str) -> IResult<&str, Subquery> {
	let (i, v) = map(ifelse, Subquery::Ifelse)(i)?;
	Ok((i, v))
}

fn subquery_others(i: &str) -> IResult<&str, Subquery> {
	let (i, _) = char('(')(i)?;
	let (i, v) = alt((
		map(select, |v| Subquery::Select(Arc::new(v))),
		map(create, |v| Subquery::Create(Arc::new(v))),
		map(update, |v| Subquery::Update(Arc::new(v))),
		map(delete, |v| Subquery::Delete(Arc::new(v))),
		map(relate, |v| Subquery::Relate(Arc::new(v))),
		map(insert, |v| Subquery::Insert(Arc::new(v))),
		map(value, Subquery::Value),
	))(i)?;
	let (i, _) = char(')')(i)?;
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
