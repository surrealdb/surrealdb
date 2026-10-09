//! ML model function expression - model inference.

#[cfg(feature = "ml")]
use std::sync::Arc;

#[cfg(feature = "ml")]
use crate::catalog::MlModelDefinition;
#[cfg(not(feature = "ml"))]
use crate::exec::Error as ExecError;
use crate::exec::EvalContext;
#[cfg(feature = "ml")]
use crate::expr::Error as ExprError;
use crate::expr::FlowResult;
use crate::val::Value;

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
