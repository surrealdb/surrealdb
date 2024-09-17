use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::{Base, Ident, Object, Value, Version};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum InfoStatement {
	// revision discriminant override accounting for previous behavior when adding variants and
	// removing not at the end of the enum definition.
	#[revision(override(revision = 2, discriminant = 1), override(revision = 3, discriminant = 1))]
	Root(#[revision(start = 2)] bool),

	#[revision(override(revision = 2, discriminant = 3), override(revision = 3, discriminant = 3))]
	Ns(#[revision(start = 2)] bool),

	#[revision(override(revision = 2, discriminant = 5), override(revision = 3, discriminant = 5))]
	Db(#[revision(start = 2)] bool, #[revision(start = 5)] Option<Version>),

	#[revision(override(revision = 2, discriminant = 7), override(revision = 3, discriminant = 7))]
	Tb(Ident, #[revision(start = 2)] bool, #[revision(start = 5)] Option<Version>),

	#[revision(override(revision = 2, discriminant = 9), override(revision = 3, discriminant = 9))]
	User(Ident, Option<Base>, #[revision(start = 2)] bool),

	#[revision(start = 3)]
	#[revision(override(revision = 3, discriminant = 10))]
	Index(Ident, Ident, bool),
}

impl InfoStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
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
						"accesses".to_string() => process(txn.all_root_accesses().await?.iter().map(|v| v.redacted()).collect()),
						"namespaces".to_string() => process(txn.all_ns().await?),
						"nodes".to_string() => process(txn.all_nodes().await?),
						"users".to_string() => process(txn.all_root_users().await?),
					}),
					false => Value::from(map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_accesses().await?.iter().map(|v| v.redacted()) {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"namespaces".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns().await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"nodes".to_string() => {
							let mut out = Object::default();
							for v in txn.all_nodes().await?.iter() {
								out.insert(v.id.to_string(), v.to_string().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_users().await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::Ns(structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Get the NS
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
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"databases".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db(ns).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_users(ns).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::Db(structured, version) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Convert the version to u64 if present
				let version = version.as_ref().map(|v| v.to_u64());
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
						"tables".to_string() => process(txn.all_tb(ns, db, version).await?),
						"users".to_string() => process(txn.all_db_users(ns, db).await?),
						"configs".to_string() => process(txn.all_db_configs(ns, db).await?),
					}),
					false => Value::from(map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_accesses(ns, db).await?.iter().map(|v| v.redacted()) {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"analyzers".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_analyzers(ns, db).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"functions".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_functions(ns, db).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"models".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_models(ns, db).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"params".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_params(ns, db).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb(ns, db, version).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_users(ns, db).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"configs".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_configs(ns, db).await?.iter() {
								out.insert(v.inner.name(), v.to_string().into());
							}
							out.into()
						},
					}),
				})
			}
			InfoStatement::Tb(tb, structured, version) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Convert the version to u64 if present
				let version = version.as_ref().map(|v| v.to_u64());
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				Ok(match structured {
					true => Value::from(map! {
						"events".to_string() => process(txn.all_tb_events(ns, db, tb).await?),
						"fields".to_string() => process(txn.all_tb_fields(ns, db, tb, version).await?),
						"indexes".to_string() => process(txn.all_tb_indexes(ns, db, tb).await?),
						"lives".to_string() => process(txn.all_tb_lives(ns, db, tb).await?),
						"tables".to_string() => process(txn.all_tb_views(ns, db, tb).await?),
					}),
					false => Value::from(map! {
						"events".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_events(ns, db, tb).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"fields".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_fields(ns, db, tb, version).await?.iter() {
								out.insert(v.name.to_string(), v.to_string().into());
							}
							out.into()
						},
						"indexes".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_indexes(ns, db, tb).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"lives".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_lives(ns, db, tb).await?.iter() {
								out.insert(v.id.to_raw(), v.to_string().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_views(ns, db, tb).await?.iter() {
								out.insert(v.name.to_raw(), v.to_string().into());
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
			#[allow(unused_variables)]
			InfoStatement::Index(index, table, _structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &Base::Db)?;
				// Get the transaction
				let txn = ctx.tx();
				// Output
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(ib) = ctx.get_index_builder() {
					// Obtain the index
					let res = txn.get_tb_index(opt.ns()?, opt.db()?, table, index).await?;
					if let Some(status) = ib.get_status(&res).await {
						let mut out = Object::default();
						out.insert("building".to_string(), status.into());
						return Ok(out.into());
					}
				}
				Ok(Object::default().into())
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
			Self::Db(false, ref v) => match v {
				Some(ref v) => write!(f, "INFO FOR DATABASE VERSION {v}"),
				None => f.write_str("INFO FOR DATABASE"),
			},
			Self::Db(true, ref v) => match v {
				Some(ref v) => write!(f, "INFO FOR DATABASE VERSION {v} STRUCTURE"),
				None => f.write_str("INFO FOR DATABASE STRUCTURE"),
			},
			Self::Tb(ref t, false, ref v) => match v {
				Some(ref v) => write!(f, "INFO FOR TABLE {t} VERSION {v}"),
				None => write!(f, "INFO FOR TABLE {t}"),
			},

			Self::Tb(ref t, true, ref v) => match v {
				Some(ref v) => write!(f, "INFO FOR TABLE {t} VERSION {v} STRUCTURE"),
				None => write!(f, "INFO FOR TABLE {t} STRUCTURE"),
			},
			Self::User(ref u, ref b, false) => match b {
				Some(ref b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
			Self::User(ref u, ref b, true) => match b {
				Some(ref b) => write!(f, "INFO FOR USER {u} ON {b} STRUCTURE"),
				None => write!(f, "INFO FOR USER {u} STRUCTURE"),
			},
			Self::Index(ref i, ref t, false) => write!(f, "INFO FOR INDEX {i} ON {t}"),
			Self::Index(ref i, ref t, true) => write!(f, "INFO FOR INDEX {i} ON {t} STRUCTURE"),
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
			InfoStatement::Db(_, v) => InfoStatement::Db(true, v),
			InfoStatement::Tb(t, _, v) => InfoStatement::Tb(t, true, v),
			InfoStatement::User(u, b, _) => InfoStatement::User(u, b, true),
			InfoStatement::Index(i, t, _) => InfoStatement::Index(i, t, true),
		}
	}

	pub(crate) fn versionize(self, v: Version) -> Self {
		match self {
			InfoStatement::Db(s, _) => InfoStatement::Db(s, Some(v)),
			InfoStatement::Tb(t, s, _) => InfoStatement::Tb(t, s, Some(v)),
			_ => self,
		}
	}
}

fn process<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
