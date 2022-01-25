use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
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
use std::time::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Statements(pub Vec<Statement>);

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
	Set(SetStatement),
	Use(UseStatement),
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
			Statement::Select(ref v) => match &v.timeout {
				Some(v) => Some(v.expr.value),
				None => None,
			},
			Statement::Create(ref v) => match &v.timeout {
				Some(v) => Some(v.expr.value),
				None => None,
			},
			Statement::Update(ref v) => match &v.timeout {
				Some(v) => Some(v.expr.value),
				None => None,
			},
			Statement::Relate(ref v) => match &v.timeout {
				Some(v) => Some(v.expr.value),
				None => None,
			},
			Statement::Delete(ref v) => match &v.timeout {
				Some(v) => Some(v.expr.value),
				None => None,
			},
			Statement::Insert(ref v) => match &v.timeout {
				Some(v) => Some(v.expr.value),
				None => None,
			},
			_ => None,
		}
	}
}

impl Statement {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self {
			Statement::Set(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Info(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Live(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Kill(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Output(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Ifelse(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Select(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Create(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Update(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Relate(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Delete(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Insert(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Define(ref v) => v.compute(ctx, opt, exe, doc).await,
			Statement::Remove(ref v) => v.compute(ctx, opt, exe, doc).await,
			_ => unreachable!(),
		}
	}
}

impl fmt::Display for Statement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Statement::Use(ref v) => write!(f, "{}", v),
			Statement::Set(ref v) => write!(f, "{}", v),
			Statement::Info(ref v) => write!(f, "{}", v),
			Statement::Live(ref v) => write!(f, "{}", v),
			Statement::Kill(ref v) => write!(f, "{}", v),
			Statement::Begin(ref v) => write!(f, "{}", v),
			Statement::Cancel(ref v) => write!(f, "{}", v),
			Statement::Commit(ref v) => write!(f, "{}", v),
			Statement::Output(ref v) => write!(f, "{}", v),
			Statement::Ifelse(ref v) => write!(f, "{}", v),
			Statement::Select(ref v) => write!(f, "{}", v),
			Statement::Create(ref v) => write!(f, "{}", v),
			Statement::Update(ref v) => write!(f, "{}", v),
			Statement::Relate(ref v) => write!(f, "{}", v),
			Statement::Delete(ref v) => write!(f, "{}", v),
			Statement::Insert(ref v) => write!(f, "{}", v),
			Statement::Define(ref v) => write!(f, "{}", v),
			Statement::Remove(ref v) => write!(f, "{}", v),
			Statement::Option(ref v) => write!(f, "{}", v),
		}
	}
}

pub fn statement(i: &str) -> IResult<&str, Statement> {
	delimited(
		mightbespace,
		alt((
			map(set, |v| Statement::Set(v)),
			map(yuse, |v| Statement::Use(v)),
			map(info, |v| Statement::Info(v)),
			map(live, |v| Statement::Live(v)),
			map(kill, |v| Statement::Kill(v)),
			map(begin, |v| Statement::Begin(v)),
			map(cancel, |v| Statement::Cancel(v)),
			map(commit, |v| Statement::Commit(v)),
			map(output, |v| Statement::Output(v)),
			map(ifelse, |v| Statement::Ifelse(v)),
			map(select, |v| Statement::Select(v)),
			map(create, |v| Statement::Create(v)),
			map(update, |v| Statement::Update(v)),
			map(relate, |v| Statement::Relate(v)),
			map(delete, |v| Statement::Delete(v)),
			map(insert, |v| Statement::Insert(v)),
			map(define, |v| Statement::Define(v)),
			map(remove, |v| Statement::Remove(v)),
			map(option, |v| Statement::Option(v)),
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
