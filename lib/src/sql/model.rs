use crate::error::Db::Thrown;
use crate::{
	ctx::Context,
	dbs::{Options, Transaction},
	doc::CursorDoc,
	err::Error,
	sql::{number::Number, value::Value},
};
use async_recursion::async_recursion;
use derive::Store;
use revision::revisioned;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt;

#[cfg(feature = "ml")]
use crate::kvs::Datastore;
#[cfg(feature = "ml")]
use crate::kvs::LockType::Optimistic;
#[cfg(feature = "ml")]
use crate::kvs::TransactionType::Read;
#[cfg(feature = "ml")]
use crate::obs::get::get_local_file;
#[cfg(feature = "ml")]
use std::collections::HashMap;
#[cfg(feature = "ml")]
use surrealml_core::execution::compute::ModelComputation;
#[cfg(feature = "ml")]
use surrealml_core::storage::surml_file::SurMlFile;

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
	/// This function unpacks a Value into a f32. This is used for unpacking the arguments passed
	/// into the ML model from the SQL statement.
	///
	/// # Arguments
	/// * `number` - The Value to be unpacked.
	///
	/// # Returns
	/// * `f32` - The unpacked value.
	pub fn unpack_number(number: &Number) -> f32 {
		match number {
			Number::Int(i) => *i as f32,
			Number::Float(f) => *f as f32,
			Number::Decimal(d) => d.to_f32().unwrap(),
		}
	}

	/// Defines the value of the key of the model for the key value store of the hash.
	///
	/// # Returns
	/// * `String` - The key of the model.
	pub fn key(&self) -> String {
		format!("{}@{}", self.name, self.version)
	}

	#[cfg(feature = "ml")]
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match &self.args[0] {
			// performing a buffered compute
			Value::Object(values) => {
				let mut map = HashMap::new();
				for key in values.keys() {
					match values.get(key).unwrap() {
						Value::Number(number) => {
							map.insert(key.to_string(), Self::unpack_number(number));
						}
						Value::Idiom(idiom) => {
							let value = idiom.compute(ctx, opt, txn, doc).await?;
							match value {
								Value::Number(number) => {
									map.insert(key.to_string(), Self::unpack_number(&number));
								}
								_ => return Err(Thrown("idiom needs to be a number".to_string())),
							}
						}
						Value::Bool(boolean) => {
							map.insert(key.to_string(), *boolean as i32 as f32);
						}
						_ => return Err(Thrown(
							"args need to be either a number or an object or a vector of numbers"
								.to_string(),
						)),
					}
				}
				// load the file hash from the Datastore
				let response: String;
				{
					let ds = Datastore::new("file://ml_cache.db").await.unwrap();
					let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
					let id = format!("{}-{}", self.name, self.version);
					response = String::from_utf8(tx.get(id).await.unwrap().unwrap()).unwrap();
				}
				// get the local file bytes from the object storage
				let file_bytes = get_local_file(response).await.unwrap();

				// run the compute in a blocking task
				let outcome = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(file_bytes).unwrap();
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.buffered_compute(&mut map).map_err(|e| Thrown(e.to_string()))
				})
				.await
				.unwrap()?;
				return Ok(Value::Number(Number::Float(outcome[0] as f64)));
			}
			// performing a raw compute
			Value::Number(_) => {
				let mut buffer = Vec::new();
				for i in self.args.iter() {
					match i {
						Value::Number(number) => {
							buffer.push(Self::unpack_number(number));
						}
						Value::Idiom(idiom) => {
							let value = idiom.compute(ctx, opt, txn, doc).await?;
							match value {
								Value::Number(number) => {
									buffer.push(Self::unpack_number(&number));
								}
								_ => return Err(Thrown("idiom needs to be a number".to_string())),
							}
						}
						Value::Bool(boolean) => {
							buffer.push(*boolean as i32 as f32);
						}
						_ => {
							println!("Not a number");
						}
					}
				}
				// load the file hash from the Datastore
				let response: String;
				{
					let ds = Datastore::new("file://ml_cache.db").await.unwrap();
					let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
					let id = format!("{}-{}", self.name, self.version);
					response = String::from_utf8(tx.get(id).await.unwrap().unwrap()).unwrap();
				}
				// get the local file bytes from the object storage
				let file_bytes = get_local_file(response).await.unwrap();
				let tensor = ndarray::arr1::<f32>(&buffer.as_slice()).into_dyn();

				// run the compute in a blocking task
				let outcome = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(file_bytes).unwrap();
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(|e| Thrown(e.to_string()))
				})
				.await
				.unwrap()?;
				return Ok(Value::Number(Number::Float(outcome[0] as f64)));
			}
			_ => {
				return Err(Thrown(
					"args need to be either a number or an object or a vector of numbers"
						.to_string(),
				));
			}
		}
	}

	#[cfg(not(feature = "ml"))]
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Thrown("ML is not enabled".to_string()))
	}
}

#[cfg(test)]
mod test {

	use super::Model;
	use crate::sql::number::Number;
	use rust_decimal::Decimal;

	#[test]
	fn test_unpack_int() {
		let num = Number::Int(42);
		let result = Model::unpack_number(&num);
		assert_eq!(result, 42.0_f32);
	}

	#[test]
	fn test_unpack_float() {
		let num = Number::Float(3.14);
		let result = Model::unpack_number(&num);
		assert_eq!(result, 3.14_f32);
	}

	#[test]
	fn test_unpack_decimal() {
		let decimal = Decimal::new(314, 2); // Represents 3.14
		let num = Number::Decimal(decimal);
		let result = Model::unpack_number(&num);
		assert!((result - 3.14).abs() < f32::EPSILON);
	}
}
