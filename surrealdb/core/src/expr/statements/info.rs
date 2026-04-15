use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BucketProvider, DatabaseProvider, NamespaceProvider,
	NodeProvider, RootProvider, TableProvider, UserProvider,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::sys::INFORMATION;
use crate::val::{Datetime, Object, TableName, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum InfoStatement {
	/// Root information
	Root(bool, Option<Expr>),
	/// Namespace information
	Ns(bool, Option<Expr>),
	/// Database information
	Db(bool, Option<Expr>),
	/// Table information
	Tb(Expr, bool, Option<Expr>),

	User(Expr, Option<Base>, bool),
	/// Index information
	Index(Expr, Expr, bool),
}

impl InfoStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "InfoStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			InfoStatement::Root(structured, version) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Get the transaction
				let txn = ctx.tx();
				// Convert the version to u64 if present
				let version = match version {
					Some(v) => Some(
						stk.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.cast_to::<Datetime>()?
							.to_version_stamp(txn.timestamp_impl().as_ref())?,
					),
					_ => None,
				};
				// Create the result set
				if *structured {
					let object = map! {
						"accesses".to_string() => process(txn.all_root_accesses(version).await?),
						"defaults".to_string() => txn.get_default_config().await?
							.map(|x| x.as_ref().clone().structure())
							.unwrap_or_else(|| Value::Object(Default::default())),
						"namespaces".to_string() => process(txn.all_ns(version).await?),
						"nodes".to_string() => process(txn.all_nodes().await?),
						"system".to_string() => system().await,
						"users".to_string() => process(txn.all_root_users(version).await?),
						"config".to_string() => opt.dynamic_configuration().clone().structure()
					};
					Ok(Value::Object(Object(object)))
				} else {
					let object = map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_accesses(version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"defaults".to_string() => txn.get_default_config().await?
							.map(|x| x.as_ref().clone().structure())
							.unwrap_or_else(|| Value::Object(Default::default())),
						"namespaces".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns(version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"nodes".to_string() => {
							let mut out = Object::default();
							for v in txn.all_nodes().await?.iter() {
								out.insert(v.id.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"system".to_string() => system().await,
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_root_users(version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"config".to_string() => {
							opt.dynamic_configuration().clone().structure()
						}
					};
					Ok(Value::Object(Object(object)))
				}
			}
			InfoStatement::Ns(structured, version) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Get the NS
				let ns = ctx.expect_ns_id(opt).await?;
				// Get the transaction
				let txn = ctx.tx();
				// Convert the version to u64 if present
				let version = match version {
					Some(v) => Some(
						stk.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.cast_to::<Datetime>()?
							.to_version_stamp(txn.timestamp_impl().as_ref())?,
					),
					_ => None,
				};
				// Create the result set
				if *structured {
					let object = map! {
						"accesses".to_string() => process(txn.all_ns_accesses(ns, version).await?),
						"databases".to_string() => process(txn.all_db(ns, version).await?),
						"users".to_string() => process(txn.all_ns_users(ns, version).await?),
					};
					Ok(Value::Object(Object(object)))
				} else {
					let object = map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_accesses(ns, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"databases".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db(ns, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_ns_users(ns, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
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
				// Get the transaction
				let txn = ctx.tx();
				// Convert the version to u64 if present
				let version = match version {
					Some(v) => Some(
						stk.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.cast_to::<Datetime>()?
							.to_version_stamp(txn.timestamp_impl().as_ref())?,
					),
					_ => None,
				};
				// Create the result set
				let res = if *structured {
					let object = map! {
						"accesses".to_string() => process(txn.all_db_accesses(ns, db, version).await?),
						"apis".to_string() => process(txn.all_db_apis(ns, db, version).await?),
						"analyzers".to_string() => process(txn.all_db_analyzers(ns, db, version).await?),
						"buckets".to_string() => process(txn.all_db_buckets(ns, db, version).await?),
						"functions".to_string() => process(txn.all_db_functions(ns, db, version).await?),
						"modules".to_string() => process_modules(ctx, ns, db, txn.all_db_modules(ns, db, version).await?).await,
						"models".to_string() => process(txn.all_db_models(ns, db, version).await?),
						"params".to_string() => process(txn.all_db_params(ns, db, version).await?),
						"tables".to_string() => process(txn.all_tb(ns, db, version).await?),
						"users".to_string() => process(txn.all_db_users(ns, db, version).await?),
						"configs".to_string() => process(txn.all_db_configs(ns, db, version).await?),
						"sequences".to_string() => process(txn.all_db_sequences(ns, db, version).await?),
					};
					Value::Object(Object(object))
				} else {
					let object = map! {
						"accesses".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_accesses(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"apis".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_apis(ns, db, version).await?.iter() {
								out.insert(v.path.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"analyzers".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_analyzers( ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"buckets".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_buckets(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"functions".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_functions(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"modules".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_modules(ns, db, version).await?.iter() {
								out.insert(v.get_storage_name()?, v.to_sql().into());
							}
							out.into()
						},
						"models".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_models(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"params".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_params(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb(ns, db, version).await?.iter() {
								out.insert(v.name.clone().into_string(), v.to_sql().into());
							}
							out.into()
						},
						"users".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_users(ns, db, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"configs".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_configs(ns, db, version).await?.iter() {
								out.insert(v.name(), v.to_sql().into());
							}
							out.into()
						},
						"sequences".to_string() => {
							let mut out = Object::default();
							for v in txn.all_db_sequences( ns, db, version).await?.iter() {
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
				// Compute table name
				let tb = TableName::new(expr_to_ident(stk, ctx, opt, doc, tb, "table name").await?);
				// Get the transaction
				let txn = ctx.tx();
				// Convert the version to u64 if present
				let version = match version {
					Some(v) => Some(
						stk.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.cast_to::<Datetime>()?
							.to_version_stamp(txn.timestamp_impl().as_ref())?,
					),
					_ => None,
				};
				// Create the result set
				Ok(if *structured {
					Value::from(map! {
						"events".to_string() => process(txn.all_tb_events(ns, db, &tb, version).await?),
						"fields".to_string() => process(txn.all_tb_fields(ns, db, &tb, version).await?),
						"indexes".to_string() => process(txn.all_tb_indexes(ns, db, &tb, version).await?),
						"lives".to_string() => process(txn.all_tb_lives(ns, db, &tb, version).await?),
						"tables".to_string() => process(txn.all_tb_views(ns, db, &tb, version).await?),
					})
				} else {
					Value::from(map! {
						"events".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_events(ns, db, &tb, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"fields".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_fields(ns, db, &tb, version).await?.iter() {
								out.insert(v.name.to_raw_string(), v.to_sql().into());
							}
							out.into()
						},
						"indexes".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_indexes(ns, db, &tb, version).await?.iter() {
								out.insert(v.name.clone(), v.to_sql().into());
							}
							out.into()
						},
						"lives".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_lives(ns, db, &tb, version).await?.iter() {
								out.insert(v.id.to_string(), v.to_sql().into());
							}
							out.into()
						},
						"tables".to_string() => {
							let mut out = Object::default();
							for v in txn.all_tb_views(ns, db, &tb, version).await?.iter() {
								out.insert(v.name.clone().into_string(), v.to_sql().into());
							}
							out.into()
						},
					})
				})
			}
			InfoStatement::User(user, base, structured) => {
				// Get the base type
				let base = (*base).unwrap_or(opt.selected_base()?);
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;
				// Compute user name
				let user = expr_to_ident(stk, ctx, opt, doc, user, "user name").await?;
				// Get the transaction
				let txn = ctx.tx();
				// Process the user
				let res = match base {
					Base::Root => txn.expect_root_user(&user).await?,
					Base::Ns => {
						let ns = txn.expect_ns_by_name(opt.ns()?).await?;
						match txn.get_ns_user(ns.namespace_id, &user, None).await? {
							Some(user) => user,
							None => {
								return Err(Error::UserNsNotFound {
									name: user,
									ns: ns.name.clone(),
								}
								.into());
							}
						}
					}
					Base::Db => {
						let (ns, db) = opt.ns_db()?;
						let Some(db_def) = txn.get_db_by_name(ns, db, None).await? else {
							return Err(Error::UserDbNotFound {
								name: user,
								ns: ns.to_string(),
								db: db.to_string(),
							}
							.into());
						};
						txn.get_db_user(db_def.namespace_id, db_def.database_id, &user, None)
							.await?
							.ok_or_else(|| Error::UserDbNotFound {
								name: user,
								ns: ns.to_string(),
								db: db.to_string(),
							})?
					}
				};
				// Ok all good
				Ok(if *structured {
					res.as_ref().clone().structure()
				} else {
					Value::from(res.as_ref().to_sql())
				})
			}
			InfoStatement::Index(index, table, _structured) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &Base::Db)?;
				// Compute table & index names
				let index = expr_to_ident(stk, ctx, opt, doc, index, "index name").await?;
				let table =
					TableName::new(expr_to_ident(stk, ctx, opt, doc, table, "table name").await?);
				// Get the transaction
				let txn = ctx.tx();

				if let Some(ib) = ctx.get_index_builder() {
					// Obtain the index
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					let ix = txn.expect_tb_index(ns, db, &table, &index).await?;
					let status = ib.get_status(ns, db, &ix).await;
					let mut out = Object::default();
					out.insert("building".to_string(), status.into());
					return Ok(out.into());
				}
				Ok(Object::default().into())
			}
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
	})
}

#[cfg(feature = "surrealism")]
fn surrealism_exports_manifest_to_value(
	exports: &surrealism_runtime::exports::ExportsManifest,
) -> Value {
	let values: Vec<Value> = exports
		.functions
		.iter()
		.map(|f| {
			let mut obj = Object::default();
			if let Some(name) = &f.name {
				obj.insert("name".to_string(), Value::from(name.clone()));
			}
			obj.insert(
				"args".to_string(),
				Value::Array(
					f.args
						.iter()
						.map(|(arg_name, kind)| {
							let mut arg_obj = Object::default();
							arg_obj.insert("name".to_string(), Value::from(arg_name.clone()));
							arg_obj.insert("kind".to_string(), Value::from(format!("{kind}")));
							Value::Object(arg_obj)
						})
						.collect(),
				),
			);
			obj.insert("returns".to_string(), Value::from(format!("{}", f.returns)));
			obj.insert("writeable".to_string(), Value::from(f.writeable));
			if let Some(comment) = &f.comment {
				obj.insert("comment".to_string(), Value::from(comment.clone()));
			}
			Value::Object(obj)
		})
		.collect();

	Value::Array(values.into())
}

#[cfg(feature = "surrealism")]
async fn get_module_exports(
	ctx: &FrozenContext,
	ns: &crate::catalog::NamespaceId,
	db: &crate::catalog::DatabaseId,
	executable: &crate::catalog::ModuleExecutable,
) -> Option<Value> {
	use crate::buc::store::ObjectKey;
	use crate::catalog::ModuleExecutable;
	use crate::surrealism::cache::SurrealismCacheLookup;

	match executable {
		ModuleExecutable::Surrealism(s) => {
			if let Ok(bucket) = ctx.get_bucket_store(*ns, *db, &s.bucket).await {
				let key = ObjectKey::new(s.key.clone());
				if let Ok(Some(bytes)) = bucket.get(&key).await
					&& let Ok(manifest) = surrealism_runtime::package::exports_manifest_from_reader(
						std::io::Cursor::new(bytes),
					) {
					return Some(surrealism_exports_manifest_to_value(&manifest));
				}
			}

			let lookup = SurrealismCacheLookup::File(ns, db, &s.bucket, &s.key);
			let runtime = match ctx.get_surrealism_runtime(lookup).await {
				Ok(r) => r,
				Err(e) => {
					tracing::trace!("Could not load module runtime for exports: {e}");
					return None;
				}
			};
			Some(surrealism_exports_manifest_to_value(runtime.exports()))
		}
		ModuleExecutable::Silo(s) => {
			let lookup =
				SurrealismCacheLookup::Silo(&s.organisation, &s.package, s.major, s.minor, s.patch);
			let runtime = match ctx.get_surrealism_runtime(lookup).await {
				Ok(r) => r,
				Err(e) => {
					tracing::trace!("Could not load module runtime for exports: {e}");
					return None;
				}
			};
			Some(surrealism_exports_manifest_to_value(runtime.exports()))
		}
	}
}

/// Process module definitions into structured Values, enriching each with
/// export signatures from the cached surrealism runtime when available.
pub(crate) async fn process_modules(
	ctx: &FrozenContext,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	modules: Arc<[crate::catalog::ModuleDefinition]>,
) -> Value {
	let mut values = Vec::with_capacity(modules.len());
	for module in modules.iter() {
		#[allow(unused_mut)]
		let mut val = module.clone().structure();
		#[cfg(feature = "surrealism")]
		if let Value::Object(ref mut obj) = val
			&& let Some(exports) = get_module_exports(ctx, &ns, &db, &module.executable).await
		{
			obj.insert("exports".to_string(), exports);
		}
		values.push(val);
	}
	#[cfg(not(feature = "surrealism"))]
	let _ = (ctx, ns, db);
	Value::Array(values.into())
}
