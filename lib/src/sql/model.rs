use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::sql::value::Value;
use crate::sql::Permission;
use derive::Store;
use futures::future::try_join_all;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[cfg(feature = "ml")]
use std::collections::HashMap;
#[cfg(feature = "ml")]
use surrealml_core::execution::compute::ModelComputation;
#[cfg(feature = "ml")]
use surrealml_core::storage::surml_file::SurMlFile;

const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct Model {
	pub name: String,
	pub version: String,
	pub args: Vec<Value>,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ml::{}<{}>(", self.name, self.version)?;
		for (idx, p) in self.args.iter().enumerate() {
			if idx != 0 {
				write!(f, ",")?;
			}
			write!(f, "{}", p)?;
		}
		write!(f, ")")
	}
}

impl Model {
	#[cfg(feature = "ml")]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Ensure futures are run
		let opt = &opt.new_with_futures(true);
		// Get the full name of this model
		let name = format!("ml::{}", self.name);
		// Check this function is allowed
		ctx.check_allowed_function(name.as_str())?;
		// Get the model definition
		let val = {
			// Claim transaction
			let mut run = txn.lock().await;
			// Get the function definition
			run.get_and_cache_db_model(opt.ns(), opt.db(), &self.name, &self.version).await?
		};
		// Check permissions
		if opt.check_perms(Action::View) {
			match &val.permissions {
				Permission::Full => (),
				Permission::None => {
					return Err(Error::FunctionPermissions {
						name: self.name.to_owned(),
					})
				}
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !e.compute(ctx, opt, txn, doc).await?.is_truthy() {
						return Err(Error::FunctionPermissions {
							name: self.name.to_owned(),
						});
					}
				}
			}
		}
		// Compute the function arguments
		let mut args =
			try_join_all(self.args.iter().map(|v| v.compute(ctx, opt, txn, doc))).await?;
		//
		if args.len() != 1 {
			return Err(Error::InvalidArguments {
				name: format!("ml::{}<{}>", self.name, self.version),
				message: ARGUMENTS.into(),
			});
		}
		//
		match args.swap_remove(0) {
			// Perform bufferered compute
			Value::Object(v) => {
				// Compute the model function arguments
				let mut args = v
					.into_iter()
					.map(|(k, v)| Ok((k, Value::try_into(v)?)))
					.collect::<Result<HashMap<String, f32>, Error>>()
					.map_err(|_| Error::InvalidArguments {
						name: format!("ml::{}<{}>", self.name, self.version),
						message: ARGUMENTS.into(),
					})?;
				// Get the model file as bytes
				let bytes = crate::obs::cache::get(&val.hash).await?;
				// Run the compute in a blocking task
				let outcome = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).unwrap();
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.buffered_compute(&mut args).map_err(Error::ModelComputation)
				})
				.await
				.unwrap()?;
				// TODO
				//
				Ok(outcome[0].into())
			}
			// Perform raw compute
			Value::Number(v) => {
				// Compute the model function arguments
				let args: f32 = v.try_into().map_err(|_| Error::InvalidArguments {
					name: format!("ml::{}<{}>", self.name, self.version),
					message: ARGUMENTS.into(),
				})?;
				// Get the model file as bytes
				let bytes = crate::obs::cache::get(&val.hash).await?;
				// Convert the argument to a tensor
				let tensor = ndarray::arr1::<f32>(&[args]).into_dyn();
				// Run the compute in a blocking task
				let outcome = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).unwrap();
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(Error::ModelComputation)
				})
				.await
				.unwrap()?;
				//
				Ok(outcome[0].into())
			}
			// Perform raw compute
			Value::Array(v) => {
				// Compute the model function arguments
				let args = v
					.into_iter()
					.map(Value::try_into)
					.collect::<Result<Vec<f32>, Error>>()
					.map_err(|_| Error::InvalidArguments {
						name: format!("ml::{}<{}>", self.name, self.version),
						message: ARGUMENTS.into(),
					})?;
				// Get the model file as bytes
				let bytes = crate::obs::cache::get(&val.hash).await?;
				// Convert the argument to a tensor
				let tensor = ndarray::arr1::<f32>(&args).into_dyn();
				// Run the compute in a blocking task
				let outcome = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).unwrap();
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(Error::ModelComputation)
				})
				.await
				.unwrap()?;
				//
				Ok(outcome[0].into())
			}
			//
			_ => Err(Error::InvalidArguments {
				name: format!("ml::{}<{}>", self.name, self.version),
				message: ARGUMENTS.into(),
			}),
		}
	}

	#[cfg(not(feature = "ml"))]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Thrown("ML is not enabled".to_string()))
	}
}
