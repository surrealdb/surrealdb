//! The host's rendering of catalog definitions for `INFO`: each definition a
//! program read, in its stored form, as the SurrealQL that defines it or as
//! its structure, and what the process reports of itself.
//!
//! Rendering is a function of a definition's value: the program does every
//! read, and the definition crosses into its Rust type through its type's
//! schema.

use std::sync::Arc;

use anyhow::anyhow;
use surrealdb_types::ToSql;

use super::program::{Definitions, ProcessPart};
use crate::catalog::FromStored;
use crate::catalog::stored::StoredField;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::statements::info::InfoStructure;
use crate::val::{Object, Value};

/// The Rust definitions of the stored definitions in `values`, each converted
/// by its stored type's schema.
fn compiled<T>(values: &[Value]) -> anyhow::Result<Vec<T>>
where
	T: FromStored,
	T::Stored: StoredField,
{
	values.iter().map(|value| T::from_stored(&T::Stored::from_vm_value(value.clone())?)).collect()
}

/// The definitions in `values`, each converted by its schema, that are their
/// own stored form.
fn decoded<T: StoredField>(values: &[Value]) -> anyhow::Result<Vec<T>> {
	values.iter().map(|value| Ok(T::from_vm_value(value.clone())?)).collect()
}

/// `definitions` as `INFO` renders a list of them: an array of structures,
/// or an object of SurrealQL keyed by name, a later name replacing an earlier.
fn listed<T>(
	definitions: Vec<T>,
	structured: bool,
	name: impl Fn(&T) -> anyhow::Result<String>,
) -> anyhow::Result<Value>
where
	T: InfoStructure,
	for<'a> &'a T: ToSql,
{
	if structured {
		return Ok(Value::Array(definitions.into_iter().map(InfoStructure::structure).collect()));
	}
	let mut out = Object::default();
	for definition in &definitions {
		out.insert(name(definition)?, Value::from(definition.to_sql()));
	}
	Ok(Value::Object(out))
}

/// `buckets` defined at `base`, as `INFO` renders a list of them: their SurrealQL
/// names the level when it is not a database.
fn listed_buckets(
	buckets: Vec<crate::catalog::BucketDefinition>,
	structured: bool,
	base: crate::expr::Base,
) -> anyhow::Result<Value> {
	if structured {
		return Ok(Value::Array(buckets.into_iter().map(InfoStructure::structure).collect()));
	}
	let mut out = Object::default();
	for bucket in &buckets {
		out.insert(
			bucket.name.to_string(),
			Value::from(bucket.to_runtime_definition_on(base).to_sql()),
		);
	}
	Ok(Value::Object(out))
}

/// Render the list of definitions in `values`.
pub(super) async fn render(
	kind: Definitions,
	structured: bool,
	values: &[Value],
	scope: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	version: Option<u64>,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	use crate::catalog;
	let rendered = match kind {
		Definitions::Access => {
			listed(compiled::<catalog::AccessDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Api => listed(compiled::<catalog::ApiDefinition>(values)?, structured, |v| {
			Ok(v.path.to_string())
		}),
		Definitions::Analyzer => {
			listed(decoded::<catalog::AnalyzerDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Bucket => {
			let buckets = compiled::<catalog::BucketDefinition>(values)?;
			let buckets = match scope {
				Some((ns, db)) => {
					buckets.into_iter().map(|bu| bu.with_stored_backend(ns, db)).collect()
				}
				None => buckets,
			};
			listed_buckets(buckets, structured, crate::expr::Base::Db)
		}
		Definitions::RootBucket => listed_buckets(
			compiled::<catalog::BucketDefinition>(values)?,
			structured,
			crate::expr::Base::Root,
		),
		Definitions::NsBucket => listed_buckets(
			compiled::<catalog::BucketDefinition>(values)?,
			structured,
			crate::expr::Base::Ns,
		),
		Definitions::Function => {
			listed(compiled::<catalog::FunctionDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Extension => {
			let modules = compiled::<catalog::ModuleDefinition>(values)?;
			if structured {
				let (ns, db) = scope.ok_or_else(|| {
					ControlFlow::Err(anyhow!("structured modules rendered with no database"))
				})?;
				// Each module's exports are read through its archive, whose
				// bucket definition is a catalog load made here, unsealed.
				return Ok(crate::kvs::catalog_reads::sealed(
					false,
					crate::fnc::module::process_modules(ctx.ctx(), ns, db, Arc::from(modules)),
				)
				.await);
			}
			listed(modules, false, |v| v.get_name())
		}
		Definitions::Model => {
			listed(compiled::<catalog::MlModelDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Param => {
			listed(compiled::<catalog::ParamDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Table => {
			listed(compiled::<catalog::TableDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Type => {
			listed(compiled::<catalog::TypeDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::User => listed(decoded::<catalog::UserDefinition>(values)?, structured, |v| {
			Ok(v.name.to_string())
		}),
		Definitions::Config => {
			listed(compiled::<catalog::ConfigDefinition>(values)?, structured, |v| Ok(v.name()))
		}
		Definitions::Sequence => {
			let sequences = decoded::<catalog::SequenceDefinition>(values)?;
			match scope {
				Some((ns, db)) if structured => {
					process_sequences(&ctx.txn(), ns, db, &sequences, version).await
				}
				_ => listed(sequences, structured, |v| Ok(v.name.to_string())),
			}
		}
		Definitions::Namespace => {
			listed(decoded::<catalog::NamespaceDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Database => {
			listed(decoded::<catalog::DatabaseDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Event => {
			listed(compiled::<catalog::EventDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Field => {
			listed(compiled::<catalog::FieldDefinition>(values)?, structured, |v| {
				Ok(v.name.to_raw_string())
			})
		}
		Definitions::Index => {
			listed(compiled::<catalog::IndexDefinition>(values)?, structured, |v| {
				Ok(v.name.to_string())
			})
		}
		Definitions::Live => {
			listed(compiled::<catalog::SubscriptionDefinition>(values)?, structured, |v| {
				Ok(v.id.to_string())
			})
		}
		Definitions::Node => {
			listed(decoded::<catalog::node::Node>(values)?, structured, |v| Ok(v.id.to_string()))
		}
		Definitions::Defaults => {
			return Err(ControlFlow::Err(anyhow!("the root's defaults rendered as a list")));
		}
	};
	rendered.map_err(ControlFlow::Err)
}

/// Render the one definition in `value`.
pub(super) fn render_one(kind: Definitions, structured: bool, value: &Value) -> FlowResult<Value> {
	use crate::catalog;
	match kind {
		Definitions::User => {
			let [user] = decoded::<catalog::UserDefinition>(std::slice::from_ref(value))
				.map_err(ControlFlow::Err)?
				.try_into()
				.map_err(|_| ControlFlow::Err(anyhow!("one user rendered as several")))?;
			Ok(if structured {
				user.structure()
			} else {
				Value::from((&user).to_sql())
			})
		}
		// Absent, the root's defaults are an empty object; present, they are
		// the `default` config's, which is the only kind stored under it.
		Definitions::Defaults => {
			if let Value::None = value {
				Ok(Value::Object(Object::default()))
			} else {
				let [config] = compiled::<catalog::ConfigDefinition>(std::slice::from_ref(value))
					.map_err(ControlFlow::Err)?
					.try_into()
					.map_err(|_| ControlFlow::Err(anyhow!("one config rendered as several")))?;
				match config {
					catalog::ConfigDefinition::Default(defaults) => Ok(defaults.structure()),
					other @ (catalog::ConfigDefinition::GraphQL(_)
					| catalog::ConfigDefinition::Api(_)) => Err(ControlFlow::Err(anyhow::Error::new(
						crate::err::EngineError::Internal(format!(
							"Expected a default config but found {other:?} instead"
						)),
					))),
				}
			}
		}
		other @ (Definitions::Access
		| Definitions::Api
		| Definitions::Analyzer
		| Definitions::Bucket
		| Definitions::RootBucket
		| Definitions::NsBucket
		| Definitions::Function
		| Definitions::Extension
		| Definitions::Model
		| Definitions::Param
		| Definitions::Table
		| Definitions::Type
		| Definitions::Config
		| Definitions::Sequence
		| Definitions::Namespace
		| Definitions::Database
		| Definitions::Event
		| Definitions::Field
		| Definitions::Index
		| Definitions::Live
		| Definitions::Node) => Err(ControlFlow::Err(anyhow!("{other:?} rendered as one definition"))),
	}
}

/// What the process reports of `part`.
pub(super) async fn process(part: ProcessPart, ctx: &ExecutionContext) -> Value {
	match part {
		ProcessPart::System => system().await,
		ProcessPart::Config => ctx.ctx().dynamic_configuration().clone().structure(),
	}
}

/// What the process reports of its own system: the `system` entry of
/// `INFO FOR ROOT`.
async fn system() -> Value {
	let info = crate::sys::INFORMATION.lock().await;
	Value::from(map! {
		"available_parallelism" => info.available_parallelism.into(),
		"cpu_usage" => info.cpu_usage.into(),
		"load_average" => info.load_average.iter().map(|x| Value::from(*x)).collect::<Vec<_>>().into(),
		"memory_usage" => info.memory_usage.into(),
		"physical_cores" => info.physical_cores.into(),
		"memory_allocated" => info.memory_allocated.into(),
		"memory_requested" => info.memory_requested.into(),
	})
}

/// Renders sequences for `INFO FOR DB STRUCTURE`, each carrying the value an
/// allocator would resume from alongside its definition.
///
/// `resume` is the lowest value no node has issued: the bound an export writes
/// as `START`, and where a sequence rebuilt from scratch continues. It is not
/// necessarily what the next `sequence::nextval` returns — a node still holding
/// a window it claimed earlier serves from that window, below this bound — so
/// it answers "what is safe to restart from", which is what repositioning a
/// sequence needs, rather than "what comes next".
///
/// The cursors are read at `version`, the same version the definitions were
/// read at, so a historical `INFO` reports the bound as it stood then.
async fn process_sequences(
	tx: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	sequences: &[crate::catalog::SequenceDefinition],
	version: Option<u64>,
) -> anyhow::Result<Value> {
	let mut values = Vec::with_capacity(sequences.len());
	for sq in sequences {
		let resume = crate::kvs::sequences::next_unissued_value(
			tx,
			ns,
			db,
			sq.name.as_str(),
			sq.start,
			version,
		)
		.await?;
		let mut val = sq.clone().structure();
		if let Value::Object(ref mut obj) = val {
			// Rendered like `start` and `batch` beside it, which `structure()`
			// already stringifies.
			obj.insert("resume", Value::from(resume).structure());
		}
		values.push(val);
	}
	Ok(Value::Array(values.into()))
}
