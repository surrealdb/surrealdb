//! ML model function expression - model inference.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

#[cfg(feature = "ml")]
use super::helpers::check_permission;
use super::helpers::{args_access_mode, args_required_context, evaluate_args, fmt_call_args};
#[cfg(feature = "ml")]
use crate::catalog::MlModelDefinition;
#[cfg(not(feature = "ml"))]
use crate::exec::Error as ExecError;
#[cfg(feature = "ml")]
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut};
#[cfg(feature = "ml")]
use crate::expr::Error as ExprError;
use crate::expr::{FlowResult, Model};
use crate::val::Value;

/// ML model function expression - model inference.
#[derive(Debug, Clone)]
pub struct ModelFunctionExec {
	pub(crate) model: Model,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// The `PERMISSIONS` clause of the definition this call last found,
	/// compiled. A call in a projection is made once per row, and compiling
	/// its guard is plan-time work; see [`CompiledGuard`] for what is kept
	/// and what is not.
	#[cfg(feature = "ml")]
	pub(crate) permission: CompiledGuard<MlModelDefinition>,
	/// The `PERMISSIONS` clause compiled with the plan, for the definition
	/// the planner read; see [`crate::exec::physical_expr::PlannedGuard`].
	#[cfg(feature = "ml")]
	pub(crate) planned_permission: crate::exec::physical_expr::PlannedGuard<MlModelDefinition>,
}
impl PhysicalExpr for ModelFunctionExec {
	fn name(&self) -> &'static str {
		"ModelFunction"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// ML models are stored in the database, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let definition = enter_model(&self.model.name, &self.model.version, &ctx).await?;
			#[cfg(feature = "ml")]
			{
				// Inside a permission predicate the call is the definer's and is
				// not checked.
				if !ctx.skip_fetch_perms
					&& ctx.exec_ctx.should_check_perms(crate::iam::Action::View)?
				{
					check_permission(
						crate::exec::guard_verdicts::GuardKind::Model,
						(&self.permission, &self.planned_permission),
						definition.permissions_body.logical_ref()?,
						None,
						&definition,
						&self.model.name,
						&ctx,
					)
					.await?;
				}
			}
			let args = evaluate_args(&self.arguments, ctx.clone()).await?;
			compute_model(&self.model.name, &self.model.version, &definition, args, &ctx).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Model functions are read-only (inference doesn't mutate)
		AccessMode::ReadOnly.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for ModelFunctionExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.model.fmt_sql(f, fmt);
		fmt_call_args(f, fmt, &self.arguments);
	}
}

/// The definition of model `name` at `version` that a call of it from `ctx`
/// reads, before its arguments are evaluated: the session may call it, the
/// context holds a database, and the catalog holds the model as of the
/// context's `VERSION`. Its `PERMISSIONS` clause is the caller's to judge.
/// Fails in a build without the machine-learning runtime.
#[cfg(feature = "ml")]
pub(crate) async fn enter_model(
	name: &str,
	version: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<Arc<MlModelDefinition>> {
	use crate::catalog::Error as CatalogError;
	use crate::catalog::providers::DatabaseProvider;

	let qualified = format!("ml::{name}");
	ctx.check_allowed_function(&qualified)?;
	let db_ctx = ctx
		.exec_ctx
		.database()
		.map_err(|_| anyhow::anyhow!("Model function '{}' requires database context", qualified))?;
	let txn = ctx.txn();
	let definition = crate::exec::vm::descriptor::object(txn.get_db_model(
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		name,
		version,
		ctx.exec_ctx.version_stamp(),
	))
	.await?
	.ok_or_else(|| CatalogError::MlNotFound {
		name: format!("{name}<{version}>"),
	})?;
	Ok(definition)
}

/// See the `ml` build's [`enter_model`]: every call fails here.
#[cfg(not(feature = "ml"))]
pub(crate) async fn enter_model(
	name: &str,
	version: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<std::convert::Infallible> {
	let _ = (name, version, ctx);
	Err(ExecError::InvalidModel {
		message: String::from("Machine learning computation is not enabled."),
	}
	.into())
}

/// The output of model `name` at `version`, `definition` as
/// [`enter_model`] read it, for `args`: its one argument a number, an object
/// of numbers or an array of numbers, computed on a blocking task.
#[cfg(feature = "ml")]
pub(crate) async fn compute_model(
	name: &str,
	version: &str,
	definition: &MlModelDefinition,
	mut args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	use surrealml_core::errors::error::SurrealError;
	use surrealml_core::execution::compute::ModelComputation;
	use surrealml_core::ndarray as mlNdarray;
	use surrealml_core::storage::surml_file::SurMlFile;

	use crate::expr::model::get_model_path;
	use crate::val::Number;

	const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

	let db_ctx = ctx
		.exec_ctx
		.database()
		.map_err(|_| anyhow::anyhow!("Model function 'ml::{}' requires database context", name))?;
	let path = get_model_path(db_ctx.ns_name(), db_ctx.db_name(), name, version, &definition.hash);
	if args.len() != 1 {
		return Err(ExprError::InvalidFunctionArguments {
			name: format!("ml::{name}<{version}>"),
			message: ARGUMENTS.into(),
		}
		.into());
	}
	let argument = args.pop().expect("single argument validated above");
	match argument {
		// Perform buffered compute (with normalizers)
		Value::Object(v) => {
			let mut args = v
				.into_iter()
				.map(|(k, v)| {
					v.coerce_to::<f64>().map(|f| (k.into_string(), f as f32)).map_err(|_| {
						ExprError::InvalidFunctionArguments {
							name: format!("ml::{}<{}>", name, version),
							message: ARGUMENTS.into(),
						}
					})
				})
				.collect::<Result<std::collections::HashMap<String, f32>, _>>()?;

			// Get the model file as bytes
			let bytes = crate::obs::get(&path).await?;

			// Run the compute in a blocking task
			let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
				let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
					anyhow::anyhow!("Failed to load model: {}", err.message)
				})?;
				let compute_unit = ModelComputation {
					surml_file: &mut file,
				};
				compute_unit.buffered_compute(&mut args).map_err(|err: SurrealError| {
					anyhow::anyhow!("Model computation failed: {}", err.message)
				})
			})
			.await
			.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))??;

			// Convert the output to a value
			Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
		}
		// Perform raw compute (number input)
		Value::Number(v) => {
			let args: f32 = Value::Number(v).coerce_to::<f64>().map_err(|_| {
				ExprError::InvalidFunctionArguments {
					name: format!("ml::{}<{}>", name, version),
					message: ARGUMENTS.into(),
				}
			})? as f32;

			// Get the model file as bytes
			let bytes = crate::obs::get(&path).await?;

			// Convert the argument to a tensor
			let tensor = mlNdarray::arr1::<f32>(&[args]).into_dyn();

			// Run the compute in a blocking task
			let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
				let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
					anyhow::anyhow!("Failed to load model: {}", err.message)
				})?;
				let compute_unit = ModelComputation {
					surml_file: &mut file,
				};
				compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
					anyhow::anyhow!("Model computation failed: {}", err.message)
				})
			})
			.await
			.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))??;

			// Convert the output to a value
			Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
		}
		// Perform raw compute (array input)
		Value::Array(v) => {
			let args = v
				.into_iter()
				.map(|x| x.coerce_to::<f64>().map(|x| x as f32))
				.collect::<Result<Vec<f32>, _>>()
				.map_err(|_| ExprError::InvalidFunctionArguments {
					name: format!("ml::{}<{}>", name, version),
					message: ARGUMENTS.into(),
				})?;

			// Get the model file as bytes
			let bytes = crate::obs::get(&path).await?;

			// Convert the argument to a tensor
			let tensor = mlNdarray::arr1::<f32>(&args).into_dyn();

			// Run the compute in a blocking task
			let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
				let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
					anyhow::anyhow!("Failed to load model: {}", err.message)
				})?;
				let compute_unit = ModelComputation {
					surml_file: &mut file,
				};
				compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
					anyhow::anyhow!("Model computation failed: {}", err.message)
				})
			})
			.await
			.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))??;

			// Convert the output to a value
			Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
		}
		// Invalid argument type
		_ => Err(ExprError::InvalidFunctionArguments {
			name: format!("ml::{}<{}>", name, version),
			message: ARGUMENTS.into(),
		}
		.into()),
	}
}

/// See the `ml` build's [`compute_model`]: [`enter_model`] refuses every call
/// before this is reached.
#[cfg(not(feature = "ml"))]
pub(crate) async fn compute_model(
	name: &str,
	version: &str,
	definition: &std::convert::Infallible,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	let _ = (name, version, args, ctx);
	match *definition {}
}
