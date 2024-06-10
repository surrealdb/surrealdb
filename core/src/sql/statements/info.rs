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
use std::sync::Arc;

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
		match self {
			InfoStatement::Root(structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				Ok(match structured {
					true => Value::from(map! {
						"namespaces".to_string() => process(txn.all_ns().await?),
						"users".to_string() => process(txn.all_root_users().await?),
					}),
					false => Value::from(map! {
						"namespaces".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns().await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_users().await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::Ns(structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Get the NS and DB
				let ns = opt.ns()?;
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				Ok(match structured {
					true => Value::from(map! {
						"accesses".to_string() => process(txn.all_ns_accesses(ns).await?.iter().map(|v| v.redacted()).collect()),
						"databases".to_string() => process(txn.all_db(ns).await?),
						"users".to_string() => process(txn.all_ns_users(ns).await?),
					}),
					false => Value::from(map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_accesses(ns).await?.iter().map(|v| v.redacted()) {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"databases".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db(ns).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_users(ns).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::Db(structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				Ok(match structured {
					true => Value::from(map! {
						"accesses".to_string() => process(txn.all_db_accesses(ns, db).await?.iter().map(|v| v.redacted()).collect()),
						"analyzers".to_string() => process(txn.all_db_analyzers(ns, db).await?),
						"functions".to_string() => process(txn.all_db_functions(ns, db).await?),
						"models".to_string() => process(txn.all_db_models(ns, db).await?),
						"params".to_string() => process(txn.all_db_params(ns, db).await?),
						"tables".to_string() => process(txn.all_tb(ns, db).await?),
						"users".to_string() => process(txn.all_db_users(ns, db).await?),
					}),
					false => Value::from(map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_accesses(ns, db).await?.iter().map(|v| v.redacted()) {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"analyzers".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_analyzers(ns, db).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"functions".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_functions(ns, db).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"models".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_models(ns, db).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"params".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_params(ns, db).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb(ns, db).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_users(ns, db).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::Tb(tb, structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				Ok(match structured {
					true => Value::from(map! {
						"events".to_string() => process(txn.all_tb_events(ns, db, tb).await?),
						"fields".to_string() => process(txn.all_tb_fields(ns, db, tb).await?),
						"indexes".to_string() => process(txn.all_tb_indexes(ns, db, tb).await?),
						"lives".to_string() => process(txn.all_tb_lives(ns, db, tb).await?),
						"tables".to_string() => process(txn.all_tb_views(ns, db, tb).await?),
					}),
					false => Value::from(map! {
						"events".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_events(ns, db, tb).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"fields".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_fields(ns, db, tb).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"indexes".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_indexes(ns, db, tb).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"lives".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_lives(ns, db, tb).await?.iter() {
								out.insert(v.id.to_string().into(), v.to_string().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_views(ns, db, tb).await?.iter() {
								out.insert(v.name.to_string().into(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::User(user, base, structured) => {
				// Get the base type
				let base = base.clone().unwrap_or(opt.selected_base()?);
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;
				// Get the transaction
				let txn = ctx.tx();
				// Process the user
				let res = match base {
					Base::Root => txn.get_root_user(user).await?,
					Base::Ns => txn.get_ns_user(opt.ns()?, user).await?,
					Base::Db => txn.get_db_user(opt.ns()?, opt.db()?, user).await?,
					_ => return Err(Error::InvalidLevel(base.to_string())),
				};
				// Ok all good
				Ok(match structured {
					true => res.as_ref().clone().structure(),
					false => Value::from(res.to_string()),
				})
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

fn process<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
