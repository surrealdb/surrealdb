use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::base::{base, Base};
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RemoveStatement {
	Namespace(RemoveNamespaceStatement),
	Database(RemoveDatabaseStatement),
	Login(RemoveLoginStatement),
	Token(RemoveTokenStatement),
	Scope(RemoveScopeStatement),
	Table(RemoveTableStatement),
	Event(RemoveEventStatement),
	Field(RemoveFieldStatement),
	Index(RemoveIndexStatement),
}

impl RemoveStatement {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self {
			RemoveStatement::Namespace(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Database(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Login(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Token(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Scope(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Table(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Event(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Field(ref v) => v.compute(ctx, opt, txn, doc).await,
			RemoveStatement::Index(ref v) => v.compute(ctx, opt, txn, doc).await,
		}
	}
}

impl fmt::Display for RemoveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			RemoveStatement::Namespace(v) => write!(f, "{}", v),
			RemoveStatement::Database(v) => write!(f, "{}", v),
			RemoveStatement::Login(v) => write!(f, "{}", v),
			RemoveStatement::Token(v) => write!(f, "{}", v),
			RemoveStatement::Scope(v) => write!(f, "{}", v),
			RemoveStatement::Table(v) => write!(f, "{}", v),
			RemoveStatement::Event(v) => write!(f, "{}", v),
			RemoveStatement::Field(v) => write!(f, "{}", v),
			RemoveStatement::Index(v) => write!(f, "{}", v),
		}
	}
}

pub fn remove(i: &str) -> IResult<&str, RemoveStatement> {
	alt((
		map(namespace, |v| RemoveStatement::Namespace(v)),
		map(database, |v| RemoveStatement::Database(v)),
		map(login, |v| RemoveStatement::Login(v)),
		map(token, |v| RemoveStatement::Token(v)),
		map(scope, |v| RemoveStatement::Scope(v)),
		map(table, |v| RemoveStatement::Table(v)),
		map(event, |v| RemoveStatement::Event(v)),
		map(field, |v| RemoveStatement::Field(v)),
		map(index, |v| RemoveStatement::Index(v)),
	))(i)
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveNamespaceStatement {
	pub name: String,
}

impl RemoveNamespaceStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Kv)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE NAMESPACE {}", self.name)
	}
}

fn namespace(i: &str) -> IResult<&str, RemoveNamespaceStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("NS"), tag_no_case("NAMESPACE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	Ok((
		i,
		RemoveNamespaceStatement {
			name,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveDatabaseStatement {
	pub name: String,
}

impl RemoveDatabaseStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Ns)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE DATABASE {}", self.name)
	}
}

fn database(i: &str) -> IResult<&str, RemoveDatabaseStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	Ok((
		i,
		RemoveDatabaseStatement {
			name,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveLoginStatement {
	pub name: String,
	pub base: Base,
}

impl RemoveLoginStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		match self.base {
			Base::Ns => opt.check(Level::Kv)?,
			Base::Db => opt.check(Level::Ns)?,
			_ => unreachable!(),
		}
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveLoginStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE LOGIN {} ON {}", self.name, self.base)
	}
}

fn login(i: &str) -> IResult<&str, RemoveLoginStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("LOGIN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, base) = base(i)?;
	Ok((
		i,
		RemoveLoginStatement {
			name,
			base,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveTokenStatement {
	pub name: String,
	pub base: Base,
}

impl RemoveTokenStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		match self.base {
			Base::Ns => opt.check(Level::Kv)?,
			Base::Db => opt.check(Level::Ns)?,
			_ => unreachable!(),
		}
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveTokenStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE TOKEN {} ON {}", self.name, self.base)
	}
}

fn token(i: &str) -> IResult<&str, RemoveTokenStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, base) = base(i)?;
	Ok((
		i,
		RemoveTokenStatement {
			name,
			base,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveScopeStatement {
	pub name: String,
}

impl RemoveScopeStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveScopeStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE SCOPE {}", self.name)
	}
}

fn scope(i: &str) -> IResult<&str, RemoveScopeStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SCOPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	Ok((
		i,
		RemoveScopeStatement {
			name,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveTableStatement {
	pub name: String,
}

impl RemoveTableStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE {}", self.name)
	}
}

fn table(i: &str) -> IResult<&str, RemoveTableStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	Ok((
		i,
		RemoveTableStatement {
			name,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveEventStatement {
	pub name: String,
	pub what: String,
}

impl RemoveEventStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT {} ON {}", self.name, self.what)
	}
}

fn event(i: &str) -> IResult<&str, RemoveEventStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("EVENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = ident_raw(i)?;
	Ok((
		i,
		RemoveEventStatement {
			name,
			what,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveFieldStatement {
	pub name: String,
	pub what: String,
}

impl RemoveFieldStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD {} ON {}", self.name, self.what)
	}
}

fn field(i: &str) -> IResult<&str, RemoveFieldStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FIELD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = ident_raw(i)?;
	Ok((
		i,
		RemoveFieldStatement {
			name,
			what,
		},
	))
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoveIndexStatement {
	pub name: String,
	pub what: String,
}

impl RemoveIndexStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Continue
		todo!()
	}
}

impl fmt::Display for RemoveIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE INDEX {} ON {}", self.name, self.what)
	}
}

fn index(i: &str) -> IResult<&str, RemoveIndexStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident_raw(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = ident_raw(i)?;
	Ok((
		i,
		RemoveIndexStatement {
			name,
			what,
		},
	))
}
