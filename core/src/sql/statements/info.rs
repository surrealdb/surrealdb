use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::{Base, Ident, Object, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum InfoStatement {
	#[revision(end = 2, convert_fn = "root_migrate")]
	Root,
	#[revision(start = 2)]
	Root(bool),
	#[revision(end = 2, convert_fn = "ns_migrate")]
	Ns,
	#[revision(start = 2)]
	Ns(bool),
	#[revision(end = 2, convert_fn = "db_migrate")]
	Db,
	#[revision(start = 2)]
	Db(bool),
	#[revision(end = 2, convert_fn = "tb_migrate")]
	Tb(Ident),
	#[revision(start = 2)]
	Tb(Ident, bool),
	#[revision(end = 2, convert_fn = "user_migrate")]
	User(Ident, Option<Base>),
	#[revision(start = 2)]
	User(Ident, Option<Base>, bool),
}

impl InfoStatement {
	fn root_migrate(_revision: u16, _: ()) -> Result<Self, revision::Error> {
		Ok(Self::Root(false))
	}

	fn ns_migrate(_revision: u16, _: ()) -> Result<Self, revision::Error> {
		Ok(Self::Ns(false))
	}

	fn db_migrate(_revision: u16, _: ()) -> Result<Self, revision::Error> {
		Ok(Self::Db(false))
	}

	fn tb_migrate(_revision: u16, n: (Ident,)) -> Result<Self, revision::Error> {
		Ok(Self::Tb(n.0, false))
	}

	fn user_migrate(
		_revision: u16,
		(i, b): (Ident, Option<Base>),
	) -> Result<Self, revision::Error> {
		Ok(Self::User(i, b, false))
	}
}

impl InfoStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		match self {
			InfoStatement::Root(false) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
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
			InfoStatement::Ns(false) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
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
				// Process the accesses
				let mut tmp = Object::default();
				for v in run.all_ns_accesses(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("accesses".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Db(false) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_db_users(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
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
				// Process the accesses
				let mut tmp = Object::default();
				for v in run.all_db_accesses(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("accesses".to_owned(), tmp.into());
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
			InfoStatement::Tb(tb, false) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
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
			InfoStatement::User(user, base, false) => {
				let base = base.clone().unwrap_or(opt.selected_base()?);
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;

				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
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
			InfoStatement::Root(true) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the namespaces
				res.insert("namespaces".to_owned(), process_arr(run.all_ns().await?));
				// Process the users
				res.insert("users".to_owned(), process_arr(run.all_root_users().await?));
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Ns(true) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the databases
				res.insert("databases".to_owned(), process_arr(run.all_db(opt.ns()).await?));
				// Process the users
				res.insert("users".to_owned(), process_arr(run.all_ns_users(opt.ns()).await?));
				// Process the accesses
				res.insert(
					"accesses".to_owned(),
					process_arr(run.all_ns_accesses(opt.ns()).await?),
				);
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Db(true) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the users
				res.insert(
					"users".to_owned(),
					process_arr(run.all_db_users(opt.ns(), opt.db()).await?),
				);
				// Process the accesses
				res.insert(
					"accesses".to_owned(),
					process_arr(run.all_db_accesses(opt.ns(), opt.db()).await?),
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
				// Process the accesses
				res.insert(
					"accesses".to_owned(),
					process_arr(run.all_db_accesses(opt.ns(), opt.db()).await?),
				);
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
			InfoStatement::Tb(tb, true) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
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
			InfoStatement::User(user, base, true) => {
				let base = base.clone().unwrap_or(opt.selected_base()?);
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;

				// Claim transaction
				let mut run = ctx.transaction()?.lock().await;
				// Process the user
				let res = match base {
					Base::Root => run.get_root_user(user).await?,
					Base::Ns => run.get_ns_user(opt.ns(), user).await?,
					Base::Db => run.get_db_user(opt.ns(), opt.db(), user).await?,
					_ => return Err(Error::InvalidLevel(base.to_string())),
				};
				// Ok all good
				Ok(res.structure())
			}
		}
	}
}

impl fmt::Display for InfoStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Root(false) => f.write_str("INFO FOR ROOT"),
			Self::Root(true) => f.write_str("INFO FOR ROOT STRUCTURE"),
			Self::Ns(false) => f.write_str("INFO FOR NAMESPACE"),
			Self::Ns(true) => f.write_str("INFO FOR NAMESPACE STRUCTURE"),
			Self::Db(false) => f.write_str("INFO FOR DATABASE"),
			Self::Db(true) => f.write_str("INFO FOR DATABASE STRUCTURE"),
			Self::Tb(ref t, false) => write!(f, "INFO FOR TABLE {t}"),
			Self::Tb(ref t, true) => write!(f, "INFO FOR TABLE {t} STRUCTURE"),
			Self::User(ref u, ref b, false) => match b {
				Some(ref b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
			Self::User(ref u, ref b, true) => match b {
				Some(ref b) => write!(f, "INFO FOR USER {u} ON {b} STRUCTURE"),
				None => write!(f, "INFO FOR USER {u} STRUCTURE"),
			},
		}
	}
}

use std::sync::Arc;

pub(crate) trait InfoStructure {
	fn structure(self) -> Value;
}

impl InfoStatement {
	pub(crate) fn structurize(self) -> Self {
		match self {
			InfoStatement::Root(_) => InfoStatement::Root(true),
			InfoStatement::Ns(_) => InfoStatement::Ns(true),
			InfoStatement::Db(_) => InfoStatement::Db(true),
			InfoStatement::Tb(t, _) => InfoStatement::Tb(t, true),
			InfoStatement::User(u, b, _) => InfoStatement::User(u, b, true),
		}
	}
}

fn process_arr<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
