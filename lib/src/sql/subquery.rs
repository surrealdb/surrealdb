use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::common::{closeparentheses, openparentheses};
use crate::sql::ending::subquery as ending;
use crate::sql::error::IResult;
use crate::sql::statements::create::{create, CreateStatement};
use crate::sql::statements::define::{define, DefineStatement};
use crate::sql::statements::delete::{delete, DeleteStatement};
use crate::sql::statements::ifelse::{ifelse, IfelseStatement};
use crate::sql::statements::insert::{insert, InsertStatement};
use crate::sql::statements::output::{output, OutputStatement};
use crate::sql::statements::relate::{relate, RelateStatement};
use crate::sql::statements::remove::{remove, RemoveStatement};
use crate::sql::statements::select::{select, SelectStatement};
use crate::sql::statements::update::{update, UpdateStatement};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt, peek};
use nom::sequence::tuple;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};

use super::comment::{mightbespace, shouldbespace};
use super::error::ExplainResultExt;
use super::util::expect_delimited;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Subquery";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Subquery")]
#[revisioned(revision = 1)]
pub enum Subquery {
	Value(Value),
	Ifelse(IfelseStatement),
	Output(OutputStatement),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
	Define(DefineStatement),
	Remove(RemoveStatement),
	// Add new variants here
}

impl PartialOrd for Subquery {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Subquery {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Value(v) => v.writeable(),
			Self::Ifelse(v) => v.writeable(),
			Self::Output(v) => v.writeable(),
			Self::Select(v) => v.writeable(),
			Self::Create(v) => v.writeable(),
			Self::Update(v) => v.writeable(),
			Self::Delete(v) => v.writeable(),
			Self::Relate(v) => v.writeable(),
			Self::Insert(v) => v.writeable(),
			Self::Define(v) => v.writeable(),
			Self::Remove(v) => v.writeable(),
		}
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Duplicate context
		let mut ctx = Context::new(ctx);
		// Add parent document
		if let Some(doc) = doc {
			ctx.add_value("parent", doc.doc.as_ref());
		}
		// Process the subquery
		match self {
			Self::Value(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Ifelse(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Output(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Define(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Remove(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Select(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Create(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Update(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Delete(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Relate(ref v) => v.compute(&ctx, opt, txn, doc).await,
			Self::Insert(ref v) => v.compute(&ctx, opt, txn, doc).await,
		}
	}
}

impl Display for Subquery {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => write!(f, "({v})"),
			Self::Output(v) => write!(f, "({v})"),
			Self::Select(v) => write!(f, "({v})"),
			Self::Create(v) => write!(f, "({v})"),
			Self::Update(v) => write!(f, "({v})"),
			Self::Delete(v) => write!(f, "({v})"),
			Self::Relate(v) => write!(f, "({v})"),
			Self::Insert(v) => write!(f, "({v})"),
			Self::Define(v) => write!(f, "({v})"),
			Self::Remove(v) => write!(f, "({v})"),
			Self::Ifelse(v) => Display::fmt(v, f),
		}
	}
}

pub fn subquery(i: &str) -> IResult<&str, Subquery> {
	alt((subquery_ifelse, subquery_other, subquery_value))(i)
}

fn subquery_ifelse(i: &str) -> IResult<&str, Subquery> {
	let (i, v) = map(ifelse, Subquery::Ifelse)(i)?;
	Ok((i, v))
}

fn subquery_value(i: &str) -> IResult<&str, Subquery> {
	expect_delimited(openparentheses, map(value, Subquery::Value), closeparentheses)(i)
}

fn subquery_other(i: &str) -> IResult<&str, Subquery> {
	alt((expect_delimited(openparentheses, subquery_inner, closeparentheses), |i| {
		let (i, v) = subquery_inner(i)?;
		let (i, _) = ending(i)?;
		let (i, _) = eat_semicolon(i)?;
		Ok((i, v))
	}))(i)
}

fn eat_semicolon(i: &str) -> IResult<&str, ()> {
	let (i, _) = opt(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, _) = tag(";")(i)?;
		let (i, _) = peek(tuple((
			shouldbespace,
			alt((tag_no_case("THEN"), tag_no_case("ELSE"), tag_no_case("END"))),
		)))(i)?;
		Ok((i, ()))
	})(i)?;
	Ok((i, ()))
}

fn subquery_inner(i: &str) -> IResult<&str, Subquery> {
	alt((
		map(output, Subquery::Output),
		map(select, Subquery::Select),
		map(create, Subquery::Create),
		map(update, Subquery::Update),
		map(delete, Subquery::Delete),
		map(relate, Subquery::Relate),
		map(insert, Subquery::Insert),
		map(define, Subquery::Define),
		map(remove, Subquery::Remove),
	))(i)
	.explain("This statement is not allowed in a subquery", disallowed_subquery_statements)
}

fn disallowed_subquery_statements(i: &str) -> IResult<&str, ()> {
	let (i, _) = alt((
		tag_no_case("ANALYZED"),
		tag_no_case("BEGIN"),
		tag_no_case("BREAK"),
		tag_no_case("CONTINUE"),
		tag_no_case("COMMIT"),
		tag_no_case("FOR"),
		tag_no_case("INFO"),
		tag_no_case("KILL"),
		tag_no_case("LIVE"),
		tag_no_case("OPTION"),
		tag_no_case("RELATE"),
		tag_no_case("SLEEP"),
		tag_no_case("THROW"),
		tag_no_case("USE"),
	))(i)?;
	Ok((i, ()))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn subquery_expression_statement() {
		let sql = "(1 + 2 + 3)";
		let res = subquery(sql);
		let out = res.unwrap().1;
		assert_eq!("(1 + 2 + 3)", format!("{}", out))
	}

	#[test]
	fn subquery_ifelse_statement() {
		let sql = "IF true THEN false END";
		let res = subquery(sql);
		let out = res.unwrap().1;
		assert_eq!("IF true THEN false END", format!("{}", out))
	}

	#[test]
	fn subquery_select_statement() {
		let sql = "(SELECT * FROM test)";
		let res = subquery(sql);
		let out = res.unwrap().1;
		assert_eq!("(SELECT * FROM test)", format!("{}", out))
	}

	#[test]
	fn subquery_define_statement() {
		let sql = "(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))",
			format!("{}", out)
		)
	}

	#[test]
	fn subquery_remove_statement() {
		let sql = "(REMOVE EVENT foo_event ON foo)";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(REMOVE EVENT foo_event ON foo)", format!("{}", out))
	}
}
