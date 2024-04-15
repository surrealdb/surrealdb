use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::{Base, Ident, Object, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub enum InfoStatement {
	Root,
	Ns,
	Db,
	Sc(Ident),
	Tb(Ident),
	User(Ident, Option<Base>),
	RootStructure,
	NsStructure,
	DbStructure,
	ScStructure(Ident),
	TbStructure(Ident),
}

impl InfoStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		match self {
			InfoStatement::Root => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the namespaces
				let mut tmp = Object::default();
				for v in run.all_ns().await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("namespaces".to_owned(), tmp.into());
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_root_users().await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Ns => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the databases
				let mut tmp = Object::default();
				for v in run.all_db(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("databases".to_owned(), tmp.into());
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_ns_users(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_ns_tokens(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Db => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_db_users(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_db_tokens(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Process the functions
				let mut tmp = Object::default();
				for v in run.all_db_functions(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("functions".to_owned(), tmp.into());
				// Process the models
				let mut tmp = Object::default();
				for v in run.all_db_models(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(format!("{}<{}>", v.name, v.version), v.to_string().into());
				}
				res.insert("models".to_owned(), tmp.into());
				// Process the params
				let mut tmp = Object::default();
				for v in run.all_db_params(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("params".to_owned(), tmp.into());
				// Process the scopes
				let mut tmp = Object::default();
				for v in run.all_sc(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("scopes".to_owned(), tmp.into());
				// Process the tables
				let mut tmp = Object::default();
				for v in run.all_tb(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tables".to_owned(), tmp.into());
				// Process the analyzers
				let mut tmp = Object::default();
				for v in run.all_db_analyzers(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("analyzers".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Sc(sc) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_sc_tokens(opt.ns(), opt.db(), sc).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Tb(tb) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the events
				let mut tmp = Object::default();
				for v in run.all_tb_events(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("events".to_owned(), tmp.into());
				// Process the fields
				let mut tmp = Object::default();
				for v in run.all_tb_fields(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("fields".to_owned(), tmp.into());
				// Process the tables
				let mut tmp = Object::default();
				for v in run.all_tb_views(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tables".to_owned(), tmp.into());
				// Process the indexes
				let mut tmp = Object::default();
				for v in run.all_tb_indexes(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("indexes".to_owned(), tmp.into());
				// Process the live queries
				let mut tmp = Object::default();
				for v in run.all_tb_lives(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.id.to_raw(), v.to_string().into());
				}
				res.insert("lives".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::User(user, base) => {
				let base = base.clone().unwrap_or(opt.selected_base()?);
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;

				// Claim transaction
				let mut run = txn.lock().await;
				// Process the user
				let res = match base {
					Base::Root => run.get_root_user(user).await?,
					Base::Ns => run.get_ns_user(opt.ns(), user).await?,
					Base::Db => run.get_db_user(opt.ns(), opt.db(), user).await?,
					_ => return Err(Error::InvalidLevel(base.to_string())),
				};
				// Ok all good
				Value::from(res.to_string()).ok()
			}
			InfoStatement::RootStructure => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the namespaces
				res.insert("namespaces".to_owned(), process_arr(run.all_ns().await?));
				// Process the users
				res.insert("users".to_owned(), process_arr(run.all_root_users().await?));
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::NsStructure => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the databases
				res.insert("databases".to_owned(), process_arr(run.all_db(opt.ns()).await?));
				// Process the users
				res.insert("users".to_owned(), process_arr(run.all_ns_users(opt.ns()).await?));
				// Process the tokens
				res.insert("tokens".to_owned(), process_arr(run.all_ns_tokens(opt.ns()).await?));
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::DbStructure => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the users
				res.insert(
					"users".to_owned(),
					process_arr(run.all_db_users(opt.ns(), opt.db()).await?),
				);
				// Process the tokens
				res.insert(
					"tokens".to_owned(),
					process_arr(run.all_db_tokens(opt.ns(), opt.db()).await?),
				);
				// Process the functions
				res.insert(
					"functions".to_owned(),
					process_arr(run.all_db_functions(opt.ns(), opt.db()).await?),
				);
				// Process the models
				res.insert(
					"models".to_owned(),
					process_arr(run.all_db_models(opt.ns(), opt.db()).await?),
				);
				// Process the params
				res.insert(
					"params".to_owned(),
					process_arr(run.all_db_params(opt.ns(), opt.db()).await?),
				);
				// Process the scopes
				res.insert("scopes".to_owned(), process_arr(run.all_sc(opt.ns(), opt.db()).await?));
				// Process the tables
				res.insert("tables".to_owned(), process_arr(run.all_tb(opt.ns(), opt.db()).await?));
				// Process the analyzers
				res.insert(
					"analyzers".to_owned(),
					process_arr(run.all_db_analyzers(opt.ns(), opt.db()).await?),
				);
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::ScStructure(sc) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the tokens
				res.insert(
					"tokens".to_owned(),
					process_arr(run.all_sc_tokens(opt.ns(), opt.db(), sc).await?),
				);
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::TbStructure(tb) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the events
				res.insert(
					"events".to_owned(),
					process_arr(run.all_tb_events(opt.ns(), opt.db(), tb).await?),
				);
				// Process the fields
				res.insert(
					"fields".to_owned(),
					process_arr(run.all_tb_fields(opt.ns(), opt.db(), tb).await?),
				);
				// Process the tables
				res.insert(
					"tables".to_owned(),
					process_arr(run.all_tb_views(opt.ns(), opt.db(), tb).await?),
				);
				// Process the indexes
				res.insert(
					"indexes".to_owned(),
					process_arr(run.all_tb_indexes(opt.ns(), opt.db(), tb).await?),
				);
				// Process the live queries
				res.insert(
					"lives".to_owned(),
					process_arr(run.all_tb_lives(opt.ns(), opt.db(), tb).await?),
				);
				// Ok all good
				Value::from(res).ok()
			}
		}
	}
}

impl fmt::Display for InfoStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Root | Self::RootStructure => f.write_str("INFO FOR ROOT"),
			Self::Ns | Self::NsStructure => f.write_str("INFO FOR NAMESPACE"),
			Self::Db | Self::DbStructure => f.write_str("INFO FOR DATABASE"),
			Self::Sc(ref s) | Self::ScStructure(ref s) => write!(f, "INFO FOR SCOPE {s}"),
			Self::Tb(ref t) | Self::TbStructure(ref t) => write!(f, "INFO FOR TABLE {t}"),
			Self::User(ref u, ref b) => match b {
				Some(ref b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
		}
	}
}

use std::sync::Arc;

fn process_arr<T>(a: Arc<[T]>) -> Value
where
	T: Serialize,
{
	Value::Array(a.iter().map(ser_to_val).collect())
}

fn vec_to_val<V>(v: Vec<V>) -> Value
where
	V: Into<Value>,
{
	Value::Array(v.into_iter().map(|i| i.into()).collect())
}

fn ser_to_val<S>(s: S) -> Value
where
	S: Serialize,
{
	let intermediate: serde_json::Value = serde_json::to_value(s).unwrap();
	serde_json::from_value(intermediate).unwrap()
}
