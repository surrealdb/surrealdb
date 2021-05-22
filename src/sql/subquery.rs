use crate::ctx::Context;
use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::expression::{expression, Expression};
use crate::sql::literal::Literal;
use crate::sql::statements::create::{create, CreateStatement};
use crate::sql::statements::delete::{delete, DeleteStatement};
use crate::sql::statements::ifelse::{ifelse, IfelseStatement};
use crate::sql::statements::insert::{insert, InsertStatement};
use crate::sql::statements::relate::{relate, RelateStatement};
use crate::sql::statements::select::{select, SelectStatement};
use crate::sql::statements::update::{update, UpdateStatement};
use crate::sql::statements::upsert::{upsert, UpsertStatement};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Subquery {
	Expression(Expression),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
	Upsert(UpsertStatement),
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
			Subquery::Expression(v) => write!(f, "({})", v),
			Subquery::Select(v) => write!(f, "({})", v),
			Subquery::Create(v) => write!(f, "({})", v),
			Subquery::Update(v) => write!(f, "({})", v),
			Subquery::Delete(v) => write!(f, "({})", v),
			Subquery::Relate(v) => write!(f, "({})", v),
			Subquery::Insert(v) => write!(f, "({})", v),
			Subquery::Upsert(v) => write!(f, "({})", v),
			Subquery::Ifelse(v) => write!(f, "{}", v),
		}
	}
}

impl dbs::Process for Subquery {
	fn process(
		&self,
		ctx: &Runtime,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		match self {
			Subquery::Expression(ref v) => v.process(ctx, exe, doc),
			Subquery::Ifelse(ref v) => v.process(ctx, exe, doc),
			Subquery::Select(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
			}
			Subquery::Create(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
			}
			Subquery::Update(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
			}
			Subquery::Delete(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
			}
			Subquery::Relate(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
			}
			Subquery::Insert(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
			}
			Subquery::Upsert(ref v) => {
				let mut ctx = Context::new(ctx);
				if doc.is_some() {
					let doc = doc.unwrap().clone();
					ctx.add_value("parent", doc);
				}
				let ctx = ctx.freeze();
				v.process(&ctx, exe, doc)
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
		map(upsert, |v| Subquery::Upsert(v)),
		map(expression, |v| Subquery::Expression(v)),
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
