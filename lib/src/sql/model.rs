use async_recursion::async_recursion;
use derive::Store;
use nom::{
	bytes::complete::{tag, take_while1},
	character::complete::i64,
	combinator::{cut, recognize},
	multi::separated_list1,
};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use rust_decimal::prelude::ToPrimitive;

use std::collections::HashMap;
use surrealml_core::execution::compute::ModelComputation;
use surrealml_core::storage::surml_file::SurMlFile;
use crate::kvs::Datastore;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::Read;
use crate::error::Db::Thrown;

use crate::{
	ctx::Context,
	dbs::{Options, Transaction},
	doc::CursorDoc,
	err::Error,
	sql::{error::IResult, value::Value, number::Number},
	obs::get::get_local_file
};

use super::{
	common::{closechevron, closeparentheses, commas, openchevron, openparentheses, val_char},
	error::{expect_tag_no_case, expected},
	util::{delimited_list1, expect_delimited},
	value::value,
};

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

	/// This function unpacks a Value into a f32. This is used for unpacking the arguements passed
	/// into the ML model from the SQL statement.
	/// 
	/// # Arguments
	/// * `number` - The Value to be unpacked.
	/// 
	/// # Returns
	/// * `f32` - The unpacked value.
	pub fn unpack_number(number: &Number) -> f32 {
		match number {
			Number::Int(i) => {
				*i as f32
			},
			Number::Float(f) => {
				*f as f32
			},
			Number::Decimal(d) => {
				d.to_f32().unwrap()
			}
		}
	}

	pub fn key(&self) -> String {
		format!("{}{}", self.name, self.version)
	}

	/// This function computes the result of an ML model from the args passed from the SQL statement.
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match &self.args[0] {
			// performing a buffered compute
			Value::Object(values) => {
				let mut map = HashMap::new();
				for key in values.keys() {
					match values.get(key).unwrap() {
						Value::Number(number) => {
							map.insert(key.to_string(), Self::unpack_number(number));
						},
						_ => {
							return Err(Thrown("args need to be either a number or an object or a vector of numbers".to_string()))
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
				
				// run the compute in a blocking task
				let outcome = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(file_bytes).unwrap();
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.buffered_compute(&mut map).map_err(|e| Thrown(e.to_string()))
				}).await.unwrap()?;
				return Ok(Value::Number(Number::Float(outcome[0] as f64)))
			},
			// performing a raw compute  
			Value::Number(_) => {
				let mut buffer = Vec::new();
				for i in self.args.iter() {
					match i {
						Value::Number(number) => {
							buffer.push(Self::unpack_number(number));
						},
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
					compute_unit.raw_compute(tensor, None).map_err(|e| {Thrown(e.to_string())})
				}).await.unwrap()?;

				// let mut surml_file = SurMlFile::from_bytes(file_bytes).unwrap();
				// let tensor = ndarray::arr1::<f32>(&buffer.as_slice()).into_dyn();
				// let compute_unit = ModelComputation {
				// 	surml_file: &mut surml_file,
				// };
				// let outcome = compute_unit.raw_compute(tensor, None).map_err(|e| {Thrown(e.to_string())})?;
				return Ok(Value::Number(Number::Float(outcome[0] as f64)))
			},
			_ => {
				return Err(Thrown("args need to be either a number or an object or a vector of numbers".to_string()));
			}
		}
	}
}

pub fn model(i: &str) -> IResult<&str, Model> {
	let (i, _) = tag("ml::")(i)?;

	cut(|i| {
		let (i, name) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;

		let (i, version) =
			expected("a version", expect_delimited(openchevron, version, closechevron))(i)?;

		let (i, args) = expected(
			"model arguments",
			delimited_list1(openparentheses, commas, value, closeparentheses),
		)(i)?;

		Ok((
			i,
			Model {
				name: name.to_owned(),
				version,
				args,
			},
		))
	})(i)
}

pub fn version(i: &str) -> IResult<&str, String> {
	use std::fmt::Write;

	let (i, major) = expected("a version number", i64)(i)?;
	let (i, _) = expect_tag_no_case(".")(i)?;
	let (i, minor) = expected("a version number", i64)(i)?;
	let (i, _) = expect_tag_no_case(".")(i)?;
	let (i, patch) = expected("a version number", i64)(i)?;

	let mut res = String::new();
	// Writing into a string can never error.
	write!(&mut res, "{major}.{minor}.{patch}").unwrap();
	Ok((i, res))
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::sql::query;

	#[test]
	fn ml_model_example() {
		let sql = r#"ml::insurance::prediction<1.0.0>({
				age: 18,
				disposable_income: "yes",
				purchased_before: true
			})
		"#;
		let res = model(sql);
		let out = res.unwrap().1.to_string();
		assert_eq!("ml::insurance::prediction<1.0.0>({ age: 18, disposable_income: 'yes', purchased_before: true })",out);
	}

	#[test]
	fn ml_model_example_in_select() {
		let sql = r"
			SELECT
			name,
			age,
			ml::insurance::prediction<1.0.0>({
				age: age,
				disposable_income: math::round(income),
				purchased_before: array::len(->purchased->property) > 0,
			}) AS likely_to_buy FROM person:tobie;
		";
		let res = query::query(sql);
		let out = res.unwrap().1.to_string();
		assert_eq!(
			"SELECT name, age, ml::insurance::prediction<1.0.0>({ age: age, disposable_income: math::round(income), purchased_before: array::len(->purchased->property) > 0 }) AS likely_to_buy FROM person:tobie;",
			out,
		);
	}

	#[test]
	fn ml_model_with_mutiple_arguments() {
		let sql = "ml::insurance::prediction<1.0.0>(1,2,3,4,);";
		let res = query::query(sql);
		let out = res.unwrap().1.to_string();
		assert_eq!("ml::insurance::prediction<1.0.0>(1,2,3,4);", out,);
	}

	#[test]
	fn ml_model_with_mutiple_arguments() {
		let sql = "ml::insurance::prediction<1.0.0>(1,2,3,4,);";
		let res = query::query(sql);
		let out = res.unwrap().1.to_string();
		assert_eq!("ml::insurance::prediction<1.0.0>(1,2,3,4);", out,);
	}

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
