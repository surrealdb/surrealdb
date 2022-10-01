use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::error::IResult;
use crate::sql::paths::ID;
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Subquery {
	Value(Value),
	Ifelse(IfelseStatement),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
}

impl PartialOrd for Subquery {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Subquery {
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Subquery::Value(v) => v.writeable(),
			Subquery::Ifelse(v) => v.writeable(),
			Subquery::Select(v) => v.writeable(),
			Subquery::Create(v) => v.writeable(),
			Subquery::Update(v) => v.writeable(),
			Subquery::Delete(v) => v.writeable(),
			Subquery::Relate(v) => v.writeable(),
			Subquery::Insert(v) => v.writeable(),
		}
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
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
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				let res = v.compute(&ctx, &opt, txn, doc).await?;
				// Process result
				match v.limit() {
					1 => match v.expr.single() {
						Some(v) => res.first().get(&ctx, &opt, txn, &v).await,
						None => res.first().ok(),
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
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, &opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
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
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, &opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
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
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, &opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
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
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, &opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
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
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, &opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
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
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		map(select, Subquery::Select),
		map(create, Subquery::Create),
		map(update, Subquery::Update),
		map(delete, Subquery::Delete),
		map(relate, Subquery::Relate),
		map(insert, Subquery::Insert),
		map(value, Subquery::Value),
	))(i)?;
	let (i, _) = mightbespace(i)?;
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
