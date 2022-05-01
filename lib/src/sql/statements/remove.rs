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
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Store)]
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
	pub(crate) async fn compute(
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
		map(namespace, RemoveStatement::Namespace),
		map(database, RemoveStatement::Database),
		map(login, RemoveStatement::Login),
		map(token, RemoveStatement::Token),
		map(scope, RemoveStatement::Scope),
		map(table, RemoveStatement::Table),
		map(event, RemoveStatement::Event),
		map(field, RemoveStatement::Field),
		map(index, RemoveStatement::Index),
	))(i)
}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveNamespaceStatement {
	pub name: String,
}

impl RemoveNamespaceStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Kv)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::ns::new(&self.name);
		run.del(key).await?;
		// Delete the resource data
		let key = crate::key::namespace::new(&self.name);
		run.delp(key, u32::MAX).await?;
		// Ok all good
		Ok(Value::None)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveDatabaseStatement {
	pub name: String,
}

impl RemoveDatabaseStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Ns)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::db::new(opt.ns(), &self.name);
		run.del(key).await?;
		// Delete the resource data
		let key = crate::key::database::new(opt.ns(), &self.name);
		run.delp(key, u32::MAX).await?;
		// Ok all good
		Ok(Value::None)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveLoginStatement {
	pub name: String,
	pub base: Base,
}

impl RemoveLoginStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self.base {
			Base::Ns => {
				// Allowed to run?
				opt.check(Level::Kv)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Delete the definition
				let key = crate::key::nl::new(opt.ns(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Allowed to run?
				opt.check(Level::Ns)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Delete the definition
				let key = crate::key::dl::new(opt.ns(), opt.db(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			_ => unreachable!(),
		}
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveTokenStatement {
	pub name: String,
	pub base: Base,
}

impl RemoveTokenStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self.base {
			Base::Ns => {
				// Allowed to run?
				opt.check(Level::Kv)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Delete the definition
				let key = crate::key::nt::new(opt.ns(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Allowed to run?
				opt.check(Level::Ns)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Delete the definition
				let key = crate::key::dt::new(opt.ns(), opt.db(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			_ => unreachable!(),
		}
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveScopeStatement {
	pub name: String,
}

impl RemoveScopeStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::sc::new(opt.ns(), opt.db(), &self.name);
		run.del(key).await?;
		// Ok all good
		Ok(Value::None)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveTableStatement {
	pub name: String,
}

impl RemoveTableStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::tb::new(opt.ns(), opt.db(), &self.name);
		run.del(key).await?;
		// Remove the resource data
		let key = crate::key::table::new(opt.ns(), opt.db(), &self.name);
		run.delp(key, u32::MAX).await?;
		// Ok all good
		Ok(Value::None)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveEventStatement {
	pub name: String,
	pub what: String,
}

impl RemoveEventStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::ev::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.del(key).await?;
		// Ok all good
		Ok(Value::None)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveFieldStatement {
	pub name: String,
	pub what: String,
}

impl RemoveFieldStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::fd::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.del(key).await?;
		// Ok all good
		Ok(Value::None)
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct RemoveIndexStatement {
	pub name: String,
	pub what: String,
}

impl RemoveIndexStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::Db)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Delete the definition
		let key = crate::key::ix::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.del(key).await?;
		// Remove the resource data
		let key = crate::key::guide::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.delp(key, u32::MAX).await?;
		// Ok all good
		Ok(Value::None)
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
