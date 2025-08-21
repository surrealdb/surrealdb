#[cfg(feature = "ml")]
use std::collections::HashMap;
use std::fmt;

use reblessive::tree::Stk;
#[cfg(feature = "ml")]
use surrealml::errors::error::SurrealError;
#[cfg(feature = "ml")]
use surrealml::execution::compute::ModelComputation;
#[cfg(feature = "ml")]
use surrealml::ndarray as mlNdarray;
#[cfg(feature = "ml")]
use surrealml::storage::surml_file::SurMlFile;

#[cfg(feature = "ml")]
use crate::catalog::Permission;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{ControlFlow, FlowResult};
#[cfg(feature = "ml")]
use crate::iam::Action;
use crate::val::Value;

#[cfg(feature = "ml")]
const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

pub fn get_model_path(ns: &str, db: &str, name: &str, version: &str, hash: &str) -> String {
	format!("ml/{ns}/{db}/{name}-{version}-{hash}.surml")
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Model {
	pub name: String,
	pub version: String,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ml::{}<{}>", self.name, self.version)
	}
}

impl Model {
	#[cfg(feature = "ml")]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		mut args: Vec<Value>,
	) -> FlowResult<Value> {
		use crate::val::{CoerceError, Number};

		// Get the full name of this model
		let name = format!("ml::{}", self.name);
		// Check this function is allowed
		ctx.check_allowed_function(name.as_str())?;
		// Get the model definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let Some(val) = ctx.tx().get_db_model(ns, db, &self.name, &self.version).await? else {
			return Err(ControlFlow::from(anyhow::Error::new(Error::MlNotFound {
				name: format!("{}<{}>", self.name, self.version),
			})));
		};

		// Calculate the model path
		let path = {
			let (ns, db) = opt.ns_db()?;
			get_model_path(ns, db, &self.name, &self.version, &val.hash)
		};
		// Check permissions
		if opt.check_perms(Action::View)? {
			match &val.permissions {
				Permission::Full => (),
				Permission::None => {
					return Err(ControlFlow::from(anyhow::Error::new(
						Error::FunctionPermissions {
							name: self.name.clone(),
						},
					)));
				}
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?.is_truthy() {
						return Err(ControlFlow::from(anyhow::Error::new(
							Error::FunctionPermissions {
								name: self.name.clone(),
							},
						)));
					}
				}
			}
		}

		// Check the minimum argument length
		if args.len() != 1 {
			return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidArguments {
				name: format!("ml::{}<{}>", self.name, self.version),
				message: ARGUMENTS.into(),
			})));
		}

		// Take the first and only specified argument
		let argument = args.pop().unwrap();
		match argument {
			// Perform bufferered compute
			Value::Object(v) => {
				// Compute the model function arguments
				let mut args = v
					.into_iter()
					.map(|(k, v)| Ok((k, v.coerce_to::<f64>()? as f32)))
					.collect::<std::result::Result<HashMap<String, f32>, CoerceError>>()
					.map_err(|_| Error::InvalidArguments {
						name: format!("ml::{}<{}>", self.name, self.version),
						message: ARGUMENTS.into(),
					})
					.map_err(anyhow::Error::new)?;
				// Get the model file as bytes
				let bytes = crate::obs::get(&path).await?;
				// Run the compute in a blocking task
				let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
						anyhow::Error::new(Error::ModelComputation(err.message.to_string()))
					})?;
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.buffered_compute(&mut args).map_err(|err: SurrealError| {
						anyhow::Error::new(Error::ModelComputation(err.message.to_string()))
					})
				})
				.await
				.unwrap()
				.map_err(ControlFlow::from)?;
				// Convert the output to a value
				Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
			}
			// Perform raw compute
			Value::Number(v) => {
				// Compute the model function arguments
				let args: f32 = Value::Number(v)
					.coerce_to::<f64>()
					.map_err(|_| Error::InvalidArguments {
						name: format!("ml::{}<{}>", self.name, self.version),
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
						anyhow::Error::new(Error::ModelComputation(err.message.to_string()))
					})?;
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
						anyhow::Error::new(Error::ModelComputation(err.message.to_string()))
					})
				})
				.await
				.unwrap()
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
					.map_err(|_| Error::InvalidArguments {
						name: format!("ml::{}<{}>", self.name, self.version),
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
						anyhow::Error::new(Error::ModelComputation(err.message.to_string()))
					})?;
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
						anyhow::Error::new(Error::ModelComputation(err.message.to_string()))
					})
				})
				.await
				.unwrap()
				.map_err(ControlFlow::from)?;
				// Convert the output to a value
				Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
			}
			//
			_ => Err(ControlFlow::from(anyhow::Error::new(Error::InvalidArguments {
				name: format!("ml::{}<{}>", self.name, self.version),
				message: ARGUMENTS.into(),
			}))),
		}
	}

	#[cfg(not(feature = "ml"))]
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		_args: Vec<Value>,
	) -> FlowResult<Value> {
		Err(ControlFlow::from(anyhow::Error::new(Error::InvalidModel {
			message: String::from("Machine learning computation is not enabled."),
		})))
	}
}
