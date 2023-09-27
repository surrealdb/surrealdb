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

use crate::{
	ctx::Context,
	dbs::{Options, Transaction},
	doc::CursorDoc,
	err::Error,
	sql::{error::IResult, value::Value},
};

use super::{
	common::{closechevron, closeparentheses, openchevron, openparentheses, val_char},
	error::{expect_tag_no_case, expected},
	util::expect_delimited,
	value::value,
};

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct Model {
	pub name: String,
	pub version: String,
	pub parameters: Value,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ml::{}<{}>({})", self.name, self.version, self.parameters)
	}
}

impl Model {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::Unimplemented("ML model evaluation not yet implemented".to_string()))
	}
}

pub fn model(i: &str) -> IResult<&str, Model> {
	let (i, _) = tag("ml::")(i)?;

	cut(|i| {
		let (i, name) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;

		let (i, version) =
			expected("a version", expect_delimited(openchevron, version, closechevron))(i)?;

		let (i, parameters) = expected(
			"model parameters",
			expect_delimited(openparentheses, value, closeparentheses),
		)(i)?;

		Ok((
			i,
			Model {
				name: name.to_owned(),
				version,
				parameters,
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
}
