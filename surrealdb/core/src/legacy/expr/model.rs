#[cfg(feature = "ml")]
use std::collections::HashMap;

use reblessive::tree::Stk;
#[cfg(feature = "ml")]
use surrealml_core::errors::error::SurrealError;
#[cfg(feature = "ml")]
use surrealml_core::execution::compute::ModelComputation;
#[cfg(feature = "ml")]
use surrealml_core::ndarray as mlNdarray;
#[cfg(feature = "ml")]
use surrealml_core::storage::surml_file::SurMlFile;

#[cfg(feature = "ml")]
use crate::catalog::{Error as CatalogError, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
#[cfg(feature = "ml")]
use crate::err::EngineError;
use crate::exec::Error as ExecError;
#[cfg(feature = "ml")]
use crate::expr::Error as ExprError;
#[cfg(feature = "ml")]
use crate::expr::model::ARGUMENTS;
use crate::expr::model::Model;
#[cfg(feature = "ml")]
use crate::expr::model::get_model_path;
use crate::expr::{ControlFlow, FlowResult};
#[cfg(feature = "ml")]
use crate::iam::Action;
use crate::val::Value;

#[cfg(feature = "ml")]
pub(crate) async fn model_compute(
	this: &Model,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	mut args: Vec<Value>,
) -> FlowResult<Value> {
	use crate::catalog::providers::DatabaseProvider;
	use crate::val::{CoerceError, Number};

	// Get the full name of this model
	let name = format!("ml::{}", this.name);
	// Check this function is allowed
	ctx.check_allowed_function(name.as_str())?;
	// Get the model definition
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let Some(val) = ctx.tx().get_db_model(ns, db, &this.name, &this.version, None).await? else {
		return Err(ControlFlow::from(anyhow::Error::new(CatalogError::MlNotFound {
			name: format!("{}<{}>", this.name, this.version),
		})));
	};

	// Calculate the model path
	let path = {
		let (ns, db) = opt.ns_db()?;
		get_model_path(ns, db, &this.name, &this.version, &val.hash)
	};
	// Check permissions
	if ctx.check_perms(opt, Action::View)? {
		match &val.permissions {
			Permission::Full => (),
			Permission::None => {
				return Err(ControlFlow::from(anyhow::Error::new(
					ExecError::FunctionPermissions {
						name: this.name.to_string(),
					},
				)));
			}
			Permission::Specific(e) => {
				// Disable permission recursion and block side effects
				let opt = &opt.new_for_permission_predicate();
				// Process the PERMISSION clause
				if !stk
					.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, doc))
					.await?
					.is_truthy()
				{
					return Err(ControlFlow::from(anyhow::Error::new(
						ExecError::FunctionPermissions {
							name: this.name.to_string(),
						},
					)));
				}
			}
		}
	}

	// Check the minimum argument length
	if args.len() != 1 {
		return Err(ControlFlow::from(anyhow::Error::new(ExprError::InvalidFunctionArguments {
			name: format!("ml::{}<{}>", this.name, this.version),
			message: ARGUMENTS.into(),
		})));
	}

	// Take the first and only specified argument
	let argument = args.pop().expect("single argument validated above");
	match argument {
		// Perform bufferered compute
		Value::Object(v) => {
			// Compute the model function arguments
			let mut args = v
				.into_iter()
				.map(|(k, v)| Ok((k.into_string(), v.coerce_to::<f64>()? as f32)))
				.collect::<std::result::Result<HashMap<String, f32>, CoerceError>>()
				.map_err(|_| ExprError::InvalidFunctionArguments {
					name: format!("ml::{}<{}>", this.name, this.version),
					message: ARGUMENTS.into(),
				})
				.map_err(anyhow::Error::new)?;
			// Get the model file as bytes
			let bytes = crate::obs::get(&path).await?;
			// Run the compute in a blocking task
			let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
				let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
					anyhow::Error::new(ExecError::Thrown(err.message))
				})?;
				let compute_unit = ModelComputation {
					surml_file: &mut file,
				};
				compute_unit.buffered_compute(&mut args).map_err(|err: SurrealError| {
					anyhow::Error::new(EngineError::Internal(err.message))
				})
			})
			.await
			.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))?
			.map_err(ControlFlow::from)?;
			// Convert the output to a value
			Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
		}
		// Perform raw compute
		Value::Number(v) => {
			// Compute the model function arguments
			let args: f32 = Value::Number(v)
				.coerce_to::<f64>()
				.map_err(|_| ExprError::InvalidFunctionArguments {
					name: format!("ml::{}<{}>", this.name, this.version),
					message: ARGUMENTS.into(),
				})
				.map_err(anyhow::Error::new)? as f32;
			// Get the model file as bytes
			let bytes = crate::obs::get(&path).await.map_err(ControlFlow::from)?;
			// Convert the argument to a tensor
			let tensor = mlNdarray::arr1::<f32>(&[args]).into_dyn();
			// Run the compute in a blocking task
			let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
				let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
					anyhow::Error::new(ExecError::Thrown(err.message))
				})?;
				let compute_unit = ModelComputation {
					surml_file: &mut file,
				};
				compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
					anyhow::Error::new(EngineError::Internal(err.message))
				})
			})
			.await
			.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))?
			.map_err(ControlFlow::from)?;
			// Convert the output to a value
			Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
		}
		// Perform raw compute
		Value::Array(v) => {
			// Compute the model function arguments
			let args = v
				.into_iter()
				.map(|x| x.coerce_to::<f64>().map(|x| x as f32))
				.collect::<std::result::Result<Vec<f32>, _>>()
				.map_err(|_| ExprError::InvalidFunctionArguments {
					name: format!("ml::{}<{}>", this.name, this.version),
					message: ARGUMENTS.into(),
				})
				.map_err(anyhow::Error::new)?;
			// Get the model file as bytes
			let bytes = crate::obs::get(&path).await?;
			// Convert the argument to a tensor
			let tensor = mlNdarray::arr1::<f32>(&args).into_dyn();
			// Run the compute in a blocking task
			let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
				let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
					anyhow::Error::new(ExecError::Thrown(err.message))
				})?;
				let compute_unit = ModelComputation {
					surml_file: &mut file,
				};
				compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
					anyhow::Error::new(EngineError::Internal(err.message))
				})
			})
			.await
			.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))?
			.map_err(ControlFlow::from)?;
			// Convert the output to a value
			Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
		}
		//
		_ => Err(ControlFlow::from(anyhow::Error::new(ExprError::InvalidFunctionArguments {
			name: format!("ml::{}<{}>", this.name, this.version),
			message: ARGUMENTS.into(),
		}))),
	}
}

#[cfg(not(feature = "ml"))]
pub(crate) async fn model_compute(
	_this: &Model,
	_stk: &mut Stk,
	_ctx: &FrozenContext,
	_opt: &Options,
	_doc: Option<&CursorDoc>,
	_args: Vec<Value>,
) -> FlowResult<Value> {
	Err(ControlFlow::from(anyhow::Error::new(ExecError::InvalidModel {
		message: String::from("Machine learning computation is not enabled."),
	})))
}
