use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{
	Base, DefineAccessStatement, DefineAnalyzerStatement, DefineUserStatement, Expr, FlowResultExt,
	Ident,
};
use crate::iam::{Action, ResourceKind};
use crate::sql::ToSql;
use crate::sys::INFORMATION;
use crate::val::{Datetime, Object, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum InfoStatement {
	// revision discriminant override accounting for previous behavior when adding variants and
	// removing not at the end of the enum definition.
	Root(bool),

	Ns(bool),

	Db(bool, Option<Expr>),

	Tb(Ident, bool, Option<Expr>),

	User(Ident, Option<Base>, bool),

	Index(Ident, Ident, bool),
}

impl InfoStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			InfoStatement::Root(structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				if *structured {
					let object = map! {
						"accesses".to_string() => process(txn.all_root_accesses().await?),
						"namespaces".to_string() => process(txn.all_ns().await?),
						"nodes".to_string() => process(txn.all_nodes().await?),
						"system".to_string() => system().await,
						"users".to_string() => process(txn.all_root_users().await?),
					};
					Ok(Value::Object(Object(object)))
				} else {
					let object = map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_accesses().await?.iter() {
								let def = DefineAccessStatement::from_definition(Base::Root, v).redact();
								out.insert(def.name.to_raw_string(), def.to_string().into());
							}
							out.into()
						},
						"namespaces".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns().await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
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
						"system".to_string() => system().await,
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_users().await?.iter() {
								out.insert(v.name.clone(),DefineUserStatement::from_definition(Base::Root,v).to_string().into());
							}
							out.into()
						}
					};
					Ok(Value::Object(Object(object)))
				}
			}
			InfoStatement::Ns(structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Get the NS
				let ns = ctx.expect_ns_id(opt).await?;
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				if *structured {
					let object = map! {
						"accesses".to_string() => process(txn.all_ns_accesses(ns).await?),
						"databases".to_string() => process(txn.all_db(ns).await?),
						"users".to_string() => process(txn.all_ns_users(ns).await?),
					};
					Ok(Value::Object(Object(object)))
				} else {
					let object = map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_accesses(ns).await?.iter() {
								let def = DefineAccessStatement::from_definition(Base::Ns, v).redact();
								out.insert(def.name.to_raw_string(), def.to_string().into());
							}
							out.into()
						},
						"databases".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db(ns).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_users(ns).await?.iter() {
								out.insert(v.name.clone(),DefineUserStatement::from_definition(Base::Ns,v).to_string().into());
							}
							out.into()
						},
					};
					Ok(Value::Object(Object(object)))
				}
			}
			InfoStatement::Db(structured, version) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Get the NS and DB
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				// Convert the version to u64 if present
				let version = match version {
					Some(v) => Some(
						stk.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.cast_to::<Datetime>()?
							.to_version_stamp()?,
					),
					_ => None,
				};
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				let res = if *structured {
					let object = map! {
						"accesses".to_string() => process(txn.all_db_accesses(ns, db).await?),
						"apis".to_string() => process(txn.all_db_apis(ns, db).await?),
						"analyzers".to_string() => process(txn.all_db_analyzers(ns, db).await?),
						"buckets".to_string() => process(txn.all_db_buckets(ns, db).await?),
						"functions".to_string() => process(txn.all_db_functions(ns, db).await?),
						"models".to_string() => process(txn.all_db_models(ns, db).await?),
						"params".to_string() => process(txn.all_db_params(ns, db).await?),
						"tables".to_string() => process(txn.all_tb(ns, db, version).await?),
						"users".to_string() => process(txn.all_db_users(ns, db).await?),
						"configs".to_string() => process(txn.all_db_configs(ns, db).await?),
						"sequences".to_string() => process(txn.all_db_sequences(ns, db).await?),
					};
					Value::Object(Object(object))
				} else {
					let object = map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_accesses(ns, db).await?.iter() {
								let def = DefineAccessStatement::from_definition(Base::Db, v).redact();
								out.insert(def.name.to_raw_string(), def.to_string().into());
							}
							out.into()
						},
						"apis".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_apis(ns, db).await?.iter() {
								out.insert(v.path.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"analyzers".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_analyzers( ns, db).await?.iter() {
								out.insert(v.name.clone(), DefineAnalyzerStatement::from_definition(v).to_string().into());
							}
							out.into()
						},
						"buckets".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_buckets(ns, db).await?.iter() {
								out.insert(v.name.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"functions".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_functions(ns, db).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"models".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_models(ns, db).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"params".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_params(ns, db).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_users(ns, db).await?.iter() {
								out.insert(v.name.clone(),DefineUserStatement::from_definition(Base::Db,v).to_string().into());
							}
							out.into()
						},
						"configs".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_configs(ns, db).await?.iter() {
								out.insert(v.name(), v.to_string().into());
							}
							out.into()
						},
						"sequences".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_sequences( ns, db).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
					};
					Value::Object(Object(object))
				};
				Ok(res)
			}
			InfoStatement::Tb(tb, structured, version) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Get the NS and DB
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				// Convert the version to u64 if present
				let version = match version {
					Some(v) => Some(
						stk.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.cast_to::<Datetime>()?
							.to_version_stamp()?,
					),
					_ => None,
				};
				// Get the transaction
				let txn = ctx.tx();
				// Create the result set
				Ok(if *structured {
					Value::from(map! {
						"events".to_string() => process(txn.all_tb_events(ns, db, tb).await?),
						"fields".to_string() => process(txn.all_tb_fields(ns, db, tb, version).await?),
						"indexes".to_string() => process(txn.all_tb_indexes(ns, db, tb).await?),
						"lives".to_string() => process(txn.all_tb_lives(ns, db, tb).await?),
						"tables".to_string() => process(txn.all_tb_views(ns, db, tb).await?),
					})
				} else {
					Value::from(map! {
						"events".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_events(ns, db, tb).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"fields".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_fields(ns, db, tb, version).await?.iter() {
								out.insert(v.name.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"indexes".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_indexes(ns, db, tb).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"lives".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_lives(ns, db, tb).await?.iter() {
								out.insert(v.id.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_views(ns, db, tb).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
					})
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
					Base::Root => txn.expect_root_user(user).await?,
					Base::Ns => {
						let ns = txn.expect_ns_by_name(opt.ns()?).await?;
						match txn.get_ns_user(ns.namespace_id, user).await? {
							Some(user) => user,
							None => {
								return Err(Error::UserNsNotFound {
									name: user.to_string(),
									ns: ns.name.clone(),
								}
								.into());
							}
						}
					}
					Base::Db => {
						let (ns, db) = opt.ns_db()?;
						let Some(db_def) = txn.get_db_by_name(ns, db).await? else {
							return Err(Error::UserDbNotFound {
								name: user.to_string(),
								ns: ns.to_string(),
								db: db.to_string(),
							}
							.into());
						};
						txn.get_db_user(db_def.namespace_id, db_def.database_id, user)
							.await?
							.ok_or_else(|| Error::UserDbNotFound {
								name: user.to_string(),
								ns: ns.to_string(),
								db: db.to_string(),
							})?
					}
				};
				// Ok all good
				Ok(if *structured {
					res.as_ref().clone().structure()
				} else {
					Value::from(DefineUserStatement::from_definition(base, &res).to_string())
				})
			}
			#[cfg_attr(target_family = "wasm", expect(unused_variables))]
			InfoStatement::Index(index, table, _structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &Base::Db)?;

				// Output
				#[cfg(not(target_family = "wasm"))]
				{
					// Get the transaction
					let txn = ctx.tx();

					if let Some(ib) = ctx.get_index_builder() {
						// Obtain the index
						let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
						let res = txn.get_tb_index(ns, db, table, index).await?;
						let status = ib.get_status(ns, db, &res).await;
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
			Self::Db(false, v) => match v {
				Some(v) => write!(f, "INFO FOR DATABASE VERSION {v}"),
				None => f.write_str("INFO FOR DATABASE"),
			},
			Self::Db(true, v) => match v {
				Some(v) => write!(f, "INFO FOR DATABASE VERSION {v} STRUCTURE"),
				None => f.write_str("INFO FOR DATABASE STRUCTURE"),
			},
			Self::Tb(t, false, v) => match v {
				Some(v) => write!(f, "INFO FOR TABLE {t} VERSION {v}"),
				None => write!(f, "INFO FOR TABLE {t}"),
			},

			Self::Tb(t, true, v) => match v {
				Some(v) => write!(f, "INFO FOR TABLE {t} VERSION {v} STRUCTURE"),
				None => write!(f, "INFO FOR TABLE {t} STRUCTURE"),
			},
			Self::User(u, b, false) => match b {
				Some(b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
			Self::User(u, b, true) => match b {
				Some(b) => write!(f, "INFO FOR USER {u} ON {b} STRUCTURE"),
				None => write!(f, "INFO FOR USER {u} STRUCTURE"),
			},
			Self::Index(i, t, false) => write!(f, "INFO FOR INDEX {i} ON {t}"),
			Self::Index(i, t, true) => write!(f, "INFO FOR INDEX {i} ON {t} STRUCTURE"),
		}
	}
}

pub(crate) trait InfoStructure {
	fn structure(self) -> Value;
}

fn process<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}

async fn system() -> Value {
	let info = INFORMATION.lock().await;
	Value::from(map! {
		"available_parallelism".to_string() => info.available_parallelism.into(),
		"cpu_usage".to_string() => info.cpu_usage.into(),
		"load_average".to_string() => info.load_average.iter().map(|x| Value::from(*x)).collect::<Vec<_>>().into(),
		"memory_usage".to_string() => info.memory_usage.into(),
		"physical_cores".to_string() => info.physical_cores.into(),
		"memory_allocated".to_string() => info.memory_allocated.into(),
		"threads".to_string() => info.threads.into(),
	})
}
