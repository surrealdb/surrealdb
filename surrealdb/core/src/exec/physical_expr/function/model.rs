//! ML model function expression - model inference.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context};
#[cfg(feature = "ml")]
use super::helpers::{check_permission, evaluate_args};
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::{FlowResult, Model};
use crate::val::Value;

/// ML model function expression - model inference.
#[derive(Debug, Clone)]
pub struct ModelFunctionExec {
	pub(crate) model: Model,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ModelFunctionExec {
	fn name(&self) -> &'static str {
		"ModelFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// ML models are stored in the database, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	#[cfg(feature = "ml")]
	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use surrealml_core::errors::error::SurrealError;
		use surrealml_core::execution::compute::ModelComputation;
		use surrealml_core::ndarray as mlNdarray;
		use surrealml_core::storage::surml_file::SurMlFile;

		use crate::catalog::providers::DatabaseProvider;
		use crate::expr::model::get_model_path;
		use crate::iam::Action;
		use crate::val::Number;

		const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

		// Get the full name of this model
		let name = format!("ml::{}", self.model.name);

		// Check if this function is allowed
		ctx.check_allowed_function(&name)?;

		// Get the database context for model lookup
		let db_ctx = ctx
			.exec_ctx
			.database()
			.map_err(|_| anyhow::anyhow!("Model function '{}' requires database context", name))?;

		// Get namespace and database IDs
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;

		// Get the model definition
		let val = ctx
			.txn()
			.get_db_model(ns_id, db_id, &self.model.name, &self.model.version)
			.await?
			.ok_or_else(|| Error::MlNotFound {
				name: format!("{}<{}>", self.model.name, self.model.version),
			})?;

		// Calculate the model path using namespace and database names
		let ns_name = db_ctx.ns_name();
		let db_name = db_ctx.db_name();
		let path =
			get_model_path(ns_name, db_name, &self.model.name, &self.model.version, &val.hash);

		// Check permissions
		if ctx.exec_ctx.should_check_perms(Action::View)? {
			check_permission(&val.permissions, &self.model.name, &ctx).await?;
		}

		// Evaluate arguments
		let mut args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Validate argument count
		if args.len() != 1 {
			return Err(Error::InvalidFunctionArguments {
				name: format!("ml::{}<{}>", self.model.name, self.model.version),
				message: ARGUMENTS.into(),
			}
			.into());
		}

		// Take the first and only argument
		let argument = args.pop().expect("single argument validated above");

		match argument {
			// Perform buffered compute (with normalizers)
			Value::Object(v) => {
				let mut args = v
					.into_iter()
					.map(|(k, v)| {
						v.coerce_to::<f64>().map(|f| (k, f as f32)).map_err(|_| {
							Error::InvalidFunctionArguments {
								name: format!("ml::{}<{}>", self.model.name, self.model.version),
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
					Error::InvalidFunctionArguments {
						name: format!("ml::{}<{}>", self.model.name, self.model.version),
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
					.map_err(|_| Error::InvalidFunctionArguments {
						name: format!("ml::{}<{}>", self.model.name, self.model.version),
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
			_ => Err(Error::InvalidFunctionArguments {
				name: format!("ml::{}<{}>", self.model.name, self.model.version),
				message: ARGUMENTS.into(),
			}
			.into()),
		}
	}

	#[cfg(not(feature = "ml"))]
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		Err(Error::InvalidModel {
			message: String::from("Machine learning computation is not enabled."),
		}
		.into())
	}

	fn access_mode(&self) -> AccessMode {
		// Model functions are read-only (inference doesn't mutate)
		AccessMode::ReadOnly.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for ModelFunctionExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.model.fmt_sql(f, fmt);
		f.push_str("(...)");
	}
}
