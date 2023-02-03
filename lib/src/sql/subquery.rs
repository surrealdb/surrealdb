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
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
			Self::Value(v) => v.writeable(),
			Self::Ifelse(v) => v.writeable(),
			Self::Select(v) => v.writeable(),
			Self::Create(v) => v.writeable(),
			Self::Update(v) => v.writeable(),
			Self::Delete(v) => v.writeable(),
			Self::Relate(v) => v.writeable(),
			Self::Insert(v) => v.writeable(),
		}
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Prevent deep recursion
		let opt = &opt.dive(2)?;
		// Process the subquery
		match self {
			Self::Value(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Ifelse(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Select(ref v) => {
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				let res = v.compute(&ctx, opt, txn, doc).await?;
				// Process result
				match v.limit(&ctx, opt, txn, doc).await? {
					1 => match v.expr.single() {
						Some(v) => res.first().get(&ctx, opt, txn, &v).await,
						None => res.first().ok(),
					},
					_ => match v.expr.single() {
						Some(v) => res.get(&ctx, opt, txn, &v).await,
						None => res.ok(),
					},
				}
			}
			Self::Create(ref v) => {
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
					},
					v => Ok(v),
				}
			}
			Self::Update(ref v) => {
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
					},
					v => Ok(v),
				}
			}
			Self::Delete(ref v) => {
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
					},
					v => Ok(v),
				}
			}
			Self::Relate(ref v) => {
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, opt, txn, doc).await? {
					Value::Array(mut v) => match v.len() {
						1 => Ok(v.remove(0).pick(ID.as_ref())),
						_ => Ok(Value::from(v).pick(ID.as_ref())),
					},
					v => Ok(v),
				}
			}
			Self::Insert(ref v) => {
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Add parent document
				if let Some(doc) = doc {
					ctx.add_value("parent".into(), doc);
				}
				// Process subquery
				match v.compute(&ctx, opt, txn, doc).await? {
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

impl Display for Subquery {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => write!(f, "({v})"),
			Self::Select(v) => write!(f, "({v})"),
			Self::Create(v) => write!(f, "({v})"),
			Self::Update(v) => write!(f, "({v})"),
			Self::Delete(v) => write!(f, "({v})"),
			Self::Relate(v) => write!(f, "({v})"),
			Self::Insert(v) => write!(f, "({v})"),
			Self::Ifelse(v) => Display::fmt(v, f),
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
