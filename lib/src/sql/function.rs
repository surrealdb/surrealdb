use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc;
use crate::iam::Action;
use crate::sql::comment::mightbespace;
use crate::sql::common::val_char;
use crate::sql::common::{commas, openparentheses};
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::script::{script as func, Script};
use crate::sql::value::{value, Value};
use crate::sql::Permission;
use async_recursion::async_recursion;
use futures::future::try_join_all;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::{cut, recognize};
use nom::multi::separated_list1;
use nom::sequence::terminated;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

use super::error::expected;
use super::util::delimited_list0;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[revisioned(revision = 1)]
pub enum Function {
	Normal(String, Vec<Value>),
	Custom(String, Vec<Value>),
	Script(Script, Vec<Value>),
	// Add new variants here
}

impl PartialOrd for Function {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Function {
	/// Get function name if applicable
	pub fn name(&self) -> Option<&str> {
		match self {
			Self::Normal(n, _) => Some(n.as_str()),
			Self::Custom(n, _) => Some(n.as_str()),
			_ => None,
		}
	}
	/// Get function arguments if applicable
	pub fn args(&self) -> &[Value] {
		match self {
			Self::Normal(_, a) => a,
			Self::Custom(_, a) => a,
			_ => &[],
		}
	}
	/// Convert function call to a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			Self::Script(_, _) => "function".to_string().into(),
			Self::Normal(f, _) => f.to_owned().into(),
			Self::Custom(f, _) => format!("fn::{f}").into(),
		}
	}
	/// Convert this function to an aggregate
	pub fn aggregate(&self, val: Value) -> Self {
		match self {
			Self::Normal(n, a) => {
				let mut a = a.to_owned();
				match a.len() {
					0 => a.insert(0, val),
					_ => {
						a.remove(0);
						a.insert(0, val);
					}
				}
				Self::Normal(n.to_owned(), a)
			}
			_ => unreachable!(),
		}
	}
	/// Check if this function is a custom function
	pub fn is_custom(&self) -> bool {
		matches!(self, Self::Custom(_, _))
	}

	/// Check if this function is a scripting function
	pub fn is_script(&self) -> bool {
		matches!(self, Self::Script(_, _))
	}

	/// Check if this function is a rolling function
	pub fn is_rolling(&self) -> bool {
		match self {
			Self::Normal(f, _) if f == "count" => true,
			Self::Normal(f, _) if f == "math::max" => true,
			Self::Normal(f, _) if f == "math::mean" => true,
			Self::Normal(f, _) if f == "math::min" => true,
			Self::Normal(f, _) if f == "math::sum" => true,
			Self::Normal(f, _) if f == "time::max" => true,
			Self::Normal(f, _) if f == "time::min" => true,
			_ => false,
		}
	}
	/// Check if this function is a grouping function
	pub fn is_aggregate(&self) -> bool {
		match self {
			Self::Normal(f, _) if f == "array::distinct" => true,
			Self::Normal(f, _) if f == "array::first" => true,
			Self::Normal(f, _) if f == "array::group" => true,
			Self::Normal(f, _) if f == "array::last" => true,
			Self::Normal(f, _) if f == "count" => true,
			Self::Normal(f, _) if f == "math::bottom" => true,
			Self::Normal(f, _) if f == "math::interquartile" => true,
			Self::Normal(f, _) if f == "math::max" => true,
			Self::Normal(f, _) if f == "math::mean" => true,
			Self::Normal(f, _) if f == "math::median" => true,
			Self::Normal(f, _) if f == "math::midhinge" => true,
			Self::Normal(f, _) if f == "math::min" => true,
			Self::Normal(f, _) if f == "math::mode" => true,
			Self::Normal(f, _) if f == "math::nearestrank" => true,
			Self::Normal(f, _) if f == "math::percentile" => true,
			Self::Normal(f, _) if f == "math::sample" => true,
			Self::Normal(f, _) if f == "math::spread" => true,
			Self::Normal(f, _) if f == "math::stddev" => true,
			Self::Normal(f, _) if f == "math::sum" => true,
			Self::Normal(f, _) if f == "math::top" => true,
			Self::Normal(f, _) if f == "math::trimean" => true,
			Self::Normal(f, _) if f == "math::variance" => true,
			Self::Normal(f, _) if f == "time::max" => true,
			Self::Normal(f, _) if f == "time::min" => true,
			_ => false,
		}
	}
}

impl Function {
	/// Process this type returning a computed simple Value
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Ensure futures are run
		let opt = &opt.new_with_futures(true);
		// Process the function type
		match self {
			Self::Normal(s, x) => {
				// Check this function is allowed
				ctx.check_allowed_function(s)?;
				// Compute the function arguments
				let a = try_join_all(x.iter().map(|v| v.compute(ctx, opt, txn, doc))).await?;
				// Run the normal function
				fnc::run(ctx, opt, txn, doc, s, a).await
			}
			Self::Custom(s, x) => {
				// Check this function is allowed
				ctx.check_allowed_function(format!("fn::{s}").as_str())?;
				// Get the function definition
				let val = {
					// Claim transaction
					let mut run = txn.lock().await;
					// Get the function definition
					run.get_and_cache_db_function(opt.ns(), opt.db(), s).await?
				};
				// Check permissions
				if opt.check_perms(Action::View) {
					match &val.permissions {
						Permission::Full => (),
						Permission::None => {
							return Err(Error::FunctionPermissions {
								name: s.to_owned(),
							})
						}
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Process the PERMISSION clause
							if !e.compute(ctx, opt, txn, doc).await?.is_truthy() {
								return Err(Error::FunctionPermissions {
									name: s.to_owned(),
								});
							}
						}
					}
				}
				// Return the value
				// Check the function arguments
				if x.len() != val.args.len() {
					return Err(Error::InvalidArguments {
						name: format!("fn::{}", val.name),
						message: match val.args.len() {
							1 => String::from("The function expects 1 argument."),
							l => format!("The function expects {l} arguments."),
						},
					});
				}
				// Compute the function arguments
				let a = try_join_all(x.iter().map(|v| v.compute(ctx, opt, txn, doc))).await?;
				// Duplicate context
				let mut ctx = Context::new(ctx);
				// Process the function arguments
				for (val, (name, kind)) in a.into_iter().zip(&val.args) {
					ctx.add_value(name.to_raw(), val.coerce_to(kind)?);
				}
				// Run the custom function
				val.block.compute(&ctx, opt, txn, doc).await
			}
			#[allow(unused_variables)]
			Self::Script(s, x) => {
				#[cfg(feature = "scripting")]
				{
					// Check if scripting is allowed
					ctx.check_allowed_scripting()?;
					// Compute the function arguments
					let a = try_join_all(x.iter().map(|v| v.compute(ctx, opt, txn, doc))).await?;
					// Run the script function
					fnc::script::run(ctx, opt, txn, doc, s, a).await
				}
				#[cfg(not(feature = "scripting"))]
				{
					Err(Error::InvalidScript {
						message: String::from("Embedded functions are not enabled."),
					})
				}
			}
		}
	}
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Normal(s, e) => write!(f, "{s}({})", Fmt::comma_separated(e)),
			Self::Custom(s, e) => write!(f, "fn::{s}({})", Fmt::comma_separated(e)),
			Self::Script(s, e) => write!(f, "function({}) {{{s}}}", Fmt::comma_separated(e)),
		}
	}
}

pub fn defined_function(i: &str) -> IResult<&str, Function> {
	alt((custom, script))(i)
}

pub fn builtin_function<'a>(name: &'a str, i: &'a str) -> IResult<&'a str, Function> {
	let (i, a) = expected(
		"function arguments",
		delimited_list0(openparentheses, commas, terminated(cut(value), mightbespace), char(')')),
	)(i)?;
	Ok((i, Function::Normal(name.to_string(), a)))
}

pub fn custom(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn::")(i)?;
	cut(|i| {
		let (i, s) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, a) = expected(
			"function arguments",
			delimited_list0(
				cut(openparentheses),
				commas,
				terminated(cut(value), mightbespace),
				char(')'),
			),
		)(i)?;
		Ok((i, Function::Custom(s.to_string(), a)))
	})(i)
}

fn script(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("function")(i)?;
	cut(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, a) = delimited_list0(
			openparentheses,
			commas,
			terminated(cut(value), mightbespace),
			char(')'),
		)(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char('{')(i)?;
		let (i, v) = func(i)?;
		let (i, _) = char('}')(i)?;
		Ok((i, Function::Script(v, a)))
	})(i)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::{
		builtin::{builtin_name, BuiltinName},
		test::Parse,
	};

	fn function(i: &str) -> IResult<&str, Function> {
		alt((defined_function, |i| {
			let (i, name) = builtin_name(i)?;
			let BuiltinName::Function(x) = name else {
				panic!("not a function")
			};
			builtin_function(x, i)
		}))(i)
	}

	#[test]
	fn function_single() {
		let sql = "count()";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("count()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("count"), vec![]));
	}

	#[test]
	fn function_single_not() {
		let sql = "not(10)";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("not(10)", format!("{}", out));
		assert_eq!(out, Function::Normal("not".to_owned(), vec![10.into()]));
	}

	#[test]
	fn function_module() {
		let sql = "rand::uuid()";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("rand::uuid()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("rand::uuid"), vec![]));
	}

	#[test]
	fn function_arguments() {
		let sql = "string::is::numeric(null)";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("string::is::numeric(NULL)", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("string::is::numeric"), vec![Value::Null]));
	}

	#[test]
	fn function_simple_together() {
		let sql = "function() { return 'test'; }";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("function() { return 'test'; }", format!("{}", out));
		assert_eq!(out, Function::Script(Script::parse(" return 'test'; "), vec![]));
	}

	#[test]
	fn function_simple_whitespace() {
		let sql = "function () { return 'test'; }";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("function() { return 'test'; }", format!("{}", out));
		assert_eq!(out, Function::Script(Script::parse(" return 'test'; "), vec![]));
	}

	#[test]
	fn function_script_expression() {
		let sql = "function() { return this.tags.filter(t => { return t.length > 3; }); }";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"function() { return this.tags.filter(t => { return t.length > 3; }); }",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Function::Script(
				Script::parse(" return this.tags.filter(t => { return t.length > 3; }); "),
				vec![]
			)
		);
	}
}
