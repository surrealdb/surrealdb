use crate::sql::comment::{comment, mightbespace};
use crate::sql::common::colons;
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
use crate::sql::statements::upsert::{upsert, UpsertStatement};
use crate::sql::statements::yuse::{yuse, UseStatement};
use nom::branch::alt;
use nom::combinator::map;
use nom::multi::many0;
use nom::multi::separated_nonempty_list;
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Statements(pub Vec<Statement>);

impl Statements {
	pub fn len(&self) -> usize {
		self.0.len()
	}
}

impl fmt::Display for Statements {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{};", v)).collect::<Vec<_>>().join("\n"))
	}
}

pub fn statements(i: &str) -> IResult<&str, Statements> {
	let (i, v) = separated_nonempty_list(colons, statement)(i)?;
	let (i, _) = many0(alt((colons, comment)))(i)?;
	Ok((i, Statements(v)))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
	Upsert(UpsertStatement),
	Define(DefineStatement),
	Remove(RemoveStatement),
	Option(OptionStatement),
}

impl fmt::Display for Statement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
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
			Statement::Delete(ref v) => write!(f, "{}", v),
			Statement::Relate(ref v) => write!(f, "{}", v),
			Statement::Insert(ref v) => write!(f, "{}", v),
			Statement::Upsert(ref v) => write!(f, "{}", v),
			Statement::Define(ref v) => write!(f, "{}", v),
			Statement::Remove(ref v) => write!(f, "{}", v),
			Statement::Option(ref v) => write!(f, "{}", v),
		}
	}
}

impl Statement {
	pub fn execute(&self) -> String {
		match *self {
			Statement::Use(ref v) => format!("{}", v),
			Statement::Set(ref v) => format!("{}", v),
			Statement::Info(ref v) => format!("{}", v),
			Statement::Live(ref v) => format!("{}", v),
			Statement::Kill(ref v) => format!("{}", v),
			Statement::Begin(ref v) => format!("{}", v),
			Statement::Cancel(ref v) => format!("{}", v),
			Statement::Commit(ref v) => format!("{}", v),
			Statement::Output(ref v) => format!("{}", v),
			Statement::Ifelse(ref v) => format!("{}", v),
			Statement::Select(ref v) => format!("{}", v),
			Statement::Create(ref v) => format!("{}", v),
			Statement::Update(ref v) => format!("{}", v),
			Statement::Delete(ref v) => format!("{}", v),
			Statement::Relate(ref v) => format!("{}", v),
			Statement::Insert(ref v) => format!("{}", v),
			Statement::Upsert(ref v) => format!("{}", v),
			Statement::Define(ref v) => format!("{}", v),
			Statement::Remove(ref v) => format!("{}", v),
			Statement::Option(ref v) => format!("{}", v),
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
			map(delete, |v| Statement::Delete(v)),
			map(relate, |v| Statement::Relate(v)),
			map(insert, |v| Statement::Insert(v)),
			map(upsert, |v| Statement::Upsert(v)),
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
