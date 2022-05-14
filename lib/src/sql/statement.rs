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
use std::fmt;
use std::ops::Deref;
use std::time::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Statements(pub Vec<Statement>);

impl Deref for Statements {
	type Target = Vec<Statement>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Statements {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{};", v)).collect::<Vec<_>>().join("\n"))
	}
}

pub fn statements(i: &str) -> IResult<&str, Statements> {
	let (i, v) = separated_list1(colons, statement)(i)?;
	let (i, _) = many0(alt((colons, comment)))(i)?;
	Ok((i, Statements(v)))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
			Statement::Select(v) => v.timeout.as_ref().map(|v| *v.0),
			Statement::Create(v) => v.timeout.as_ref().map(|v| *v.0),
			Statement::Update(v) => v.timeout.as_ref().map(|v| *v.0),
			Statement::Relate(v) => v.timeout.as_ref().map(|v| *v.0),
			Statement::Delete(v) => v.timeout.as_ref().map(|v| *v.0),
			Statement::Insert(v) => v.timeout.as_ref().map(|v| *v.0),
			_ => None,
		}
	}

	pub(crate) fn writeable(&self) -> bool {
		match self {
			Statement::Use(_) => false,
			Statement::Set(v) => v.writeable(),
			Statement::Info(_) => false,
			Statement::Live(_) => true,
			Statement::Kill(_) => true,
			Statement::Output(v) => v.writeable(),
			Statement::Ifelse(v) => v.writeable(),
			Statement::Select(v) => v.writeable(),
			Statement::Create(v) => v.writeable(),
			Statement::Update(v) => v.writeable(),
			Statement::Relate(v) => v.writeable(),
			Statement::Delete(v) => v.writeable(),
			Statement::Insert(v) => v.writeable(),
			Statement::Define(_) => true,
			Statement::Remove(_) => true,
			Statement::Option(_) => false,
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
			Statement::Set(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Info(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Live(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Kill(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Output(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Ifelse(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Select(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Create(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Update(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Relate(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Delete(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Insert(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Define(v) => v.compute(ctx, opt, txn, doc).await,
			Statement::Remove(v) => v.compute(ctx, opt, txn, doc).await,
			_ => unreachable!(),
		}
	}
}

impl fmt::Display for Statement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Statement::Use(v) => write!(f, "{}", v),
			Statement::Set(v) => write!(f, "{}", v),
			Statement::Info(v) => write!(f, "{}", v),
			Statement::Live(v) => write!(f, "{}", v),
			Statement::Kill(v) => write!(f, "{}", v),
			Statement::Begin(v) => write!(f, "{}", v),
			Statement::Cancel(v) => write!(f, "{}", v),
			Statement::Commit(v) => write!(f, "{}", v),
			Statement::Output(v) => write!(f, "{}", v),
			Statement::Ifelse(v) => write!(f, "{}", v),
			Statement::Select(v) => write!(f, "{}", v),
			Statement::Create(v) => write!(f, "{}", v),
			Statement::Update(v) => write!(f, "{}", v),
			Statement::Relate(v) => write!(f, "{}", v),
			Statement::Delete(v) => write!(f, "{}", v),
			Statement::Insert(v) => write!(f, "{}", v),
			Statement::Define(v) => write!(f, "{}", v),
			Statement::Remove(v) => write!(f, "{}", v),
			Statement::Option(v) => write!(f, "{}", v),
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
