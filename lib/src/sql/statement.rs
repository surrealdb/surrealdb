use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::{comment, mightbespace};
use crate::sql::common::colons;
use crate::sql::error::IResult;
use crate::sql::statements::begin::{begin, BeginStatement};
use crate::sql::statements::cancel::{cancel, CancelStatement};
use crate::sql::statements::commit::{commit, CommitStatement};
use crate::sql::statements::create::{create, CreateStatement};
use crate::sql::statements::define::{define, DefineStatement};
use crate::sql::statements::delete::{delete, DeleteStatement};
use crate::sql::statements::ifelse::{ifelse, IfelseStatement};
use crate::sql::statements::info::{info, InfoStatement};
use crate::sql::statements::insert::{insert, InsertStatement};
use crate::sql::statements::kill::{kill, KillStatement};
use crate::sql::statements::live::{live, LiveStatement};
use crate::sql::statements::option::{option, OptionStatement};
use crate::sql::statements::output::{output, OutputStatement};
use crate::sql::statements::relate::{relate, RelateStatement};
use crate::sql::statements::remove::{remove, RemoveStatement};
use crate::sql::statements::select::{select, SelectStatement};
use crate::sql::statements::set::{set, SetStatement};
use crate::sql::statements::update::{update, UpdateStatement};
use crate::sql::statements::yuse::{yuse, UseStatement};
use crate::sql::value::Value;
use nom::branch::alt;
use nom::combinator::map;
use nom::multi::many0;
use nom::multi::separated_list1;
use nom::sequence::delimited;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::time::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Statements(pub Vec<Statement>);

impl Deref for Statements {
	type Target = Vec<Statement>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Statements {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&self.0.iter().map(|ref v| format!("{};", v)).collect::<Vec<_>>().join("\n"),
			f,
		)
	}
}

pub fn statements(i: &str) -> IResult<&str, Statements> {
	let (i, v) = separated_list1(colons, statement)(i)?;
	let (i, _) = many0(alt((colons, comment)))(i)?;
	Ok((i, Statements(v)))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Statement {
	Use(UseStatement),
	Set(SetStatement),
	Info(InfoStatement),
	Live(LiveStatement),
	Kill(KillStatement),
	Begin(BeginStatement),
	Cancel(CancelStatement),
	Commit(CommitStatement),
	Output(OutputStatement),
	Ifelse(IfelseStatement),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Relate(RelateStatement),
	Delete(DeleteStatement),
	Insert(InsertStatement),
	Define(DefineStatement),
	Remove(RemoveStatement),
	Option(OptionStatement),
}

impl Statement {
	pub fn timeout(&self) -> Option<Duration> {
		match self {
			Self::Select(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Create(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Update(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Relate(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Delete(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Insert(v) => v.timeout.as_ref().map(|v| *v.0),
			_ => None,
		}
	}

	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Use(_) => false,
			Self::Set(v) => v.writeable(),
			Self::Info(_) => false,
			Self::Live(_) => true,
			Self::Kill(_) => true,
			Self::Output(v) => v.writeable(),
			Self::Ifelse(v) => v.writeable(),
			Self::Select(v) => v.writeable(),
			Self::Create(v) => v.writeable(),
			Self::Update(v) => v.writeable(),
			Self::Relate(v) => v.writeable(),
			Self::Delete(v) => v.writeable(),
			Self::Insert(v) => v.writeable(),
			Self::Define(_) => true,
			Self::Remove(_) => true,
			Self::Option(_) => false,
			_ => unreachable!(),
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
			Self::Set(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Info(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Live(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Kill(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Output(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Ifelse(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Select(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Create(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Update(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Relate(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Delete(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Insert(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Define(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Remove(v) => v.compute(ctx, opt, txn, doc).await,
			_ => unreachable!(),
		}
	}
}

impl Display for Statement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Use(v) => Display::fmt(v, f),
			Self::Set(v) => Display::fmt(v, f),
			Self::Info(v) => Display::fmt(v, f),
			Self::Live(v) => Display::fmt(v, f),
			Self::Kill(v) => Display::fmt(v, f),
			Self::Begin(v) => Display::fmt(v, f),
			Self::Cancel(v) => Display::fmt(v, f),
			Self::Commit(v) => Display::fmt(v, f),
			Self::Output(v) => Display::fmt(v, f),
			Self::Ifelse(v) => Display::fmt(v, f),
			Self::Select(v) => Display::fmt(v, f),
			Self::Create(v) => Display::fmt(v, f),
			Self::Update(v) => Display::fmt(v, f),
			Self::Relate(v) => Display::fmt(v, f),
			Self::Delete(v) => Display::fmt(v, f),
			Self::Insert(v) => Display::fmt(v, f),
			Self::Define(v) => Display::fmt(v, f),
			Self::Remove(v) => Display::fmt(v, f),
			Self::Option(v) => Display::fmt(v, f),
		}
	}
}

pub fn statement(i: &str) -> IResult<&str, Statement> {
	delimited(
		mightbespace,
		alt((
			map(set, Statement::Set),
			map(yuse, Statement::Use),
			map(info, Statement::Info),
			map(live, Statement::Live),
			map(kill, Statement::Kill),
			map(begin, Statement::Begin),
			map(cancel, Statement::Cancel),
			map(commit, Statement::Commit),
			map(output, Statement::Output),
			map(ifelse, Statement::Ifelse),
			map(select, Statement::Select),
			map(create, Statement::Create),
			map(update, Statement::Update),
			map(relate, Statement::Relate),
			map(delete, Statement::Delete),
			map(insert, Statement::Insert),
			map(define, Statement::Define),
			map(remove, Statement::Remove),
			map(option, Statement::Option),
		)),
		mightbespace,
	)(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn single_statement() {
		let sql = "CREATE test";
		let res = statement(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test", format!("{}", out))
	}

	#[test]
	fn multiple_statements() {
		let sql = "CREATE test; CREATE temp;";
		let res = statements(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_statements_semicolons() {
		let sql = "CREATE test;;;CREATE temp;;;";
		let res = statements(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}
}
