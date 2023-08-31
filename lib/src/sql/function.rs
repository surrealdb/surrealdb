use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::common::val_char;
use crate::sql::common::{commas, openparentheses};
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::script::{script as func, Script};
use crate::sql::value::{value, Value};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::{cut, recognize};
use nom::multi::separated_list1;
use nom::sequence::{preceded, terminated};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

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
	pub fn name(&self) -> &str {
		match self {
			Self::Normal(n, _) => n.as_str(),
			Self::Custom(n, _) => n.as_str(),
			_ => unreachable!(),
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
		// Prevent long function chains
		let opt = &opt.dive(1)?;
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
					run.get_fc(opt.ns(), opt.db(), s).await?
				};
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
				for (val, (name, kind)) in a.into_iter().zip(val.args) {
					ctx.add_value(name.to_raw(), val.coerce_to(&kind)?);
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

pub fn function(i: &str) -> IResult<&str, Function> {
	alt((normal, custom, script))(i)
}

pub fn normal(i: &str) -> IResult<&str, Function> {
	let (i, s) = function_names(i)?;
	let (i, a) =
		delimited_list0(openparentheses, commas, terminated(cut(value), mightbespace), char(')'))(
			i,
		)?;
	Ok((i, Function::Normal(s.to_string(), a)))
}

pub fn custom(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn::")(i)?;
	cut(|i| {
		let (i, s) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, a) = delimited_list0(
			openparentheses,
			commas,
			terminated(cut(value), mightbespace),
			char(')'),
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

pub(crate) fn function_names(i: &str) -> IResult<&str, &str> {
	recognize(alt((
		alt((
			preceded(tag("array::"), cut(function_array)),
			preceded(tag("bytes::"), cut(function_bytes)),
			preceded(tag("crypto::"), cut(function_crypto)),
			preceded(tag("duration::"), cut(function_duration)),
			preceded(tag("encoding::"), cut(function_encoding)),
			preceded(tag("geo::"), cut(function_geo)),
			preceded(tag("http::"), cut(function_http)),
			preceded(tag("is::"), cut(function_is)),
			// Don't cut in time and math for now since there are also constant's with the same
			// prefix.
			preceded(tag("math::"), function_math),
			preceded(tag("meta::"), cut(function_meta)),
			preceded(tag("parse::"), cut(function_parse)),
			preceded(tag("rand::"), cut(function_rand)),
			preceded(tag("search::"), cut(function_search)),
			preceded(tag("session::"), cut(function_session)),
			preceded(tag("string::"), cut(function_string)),
			// Don't cut in time and math for now since there are also constant's with the same
			// prefix.
			preceded(tag("time::"), function_time),
			preceded(tag("type::"), cut(function_type)),
			preceded(tag("vector::"), cut(function_vector)),
		)),
		alt((tag("count"), tag("not"), tag("rand"), tag("sleep"))),
	)))(i)
}

fn function_array(i: &str) -> IResult<&str, &str> {
	alt((
		alt((
			tag("add"),
			tag("all"),
			tag("any"),
			tag("append"),
			tag("at"),
			tag("boolean_and"),
			tag("boolean_not"),
			tag("boolean_or"),
			tag("boolean_xor"),
			tag("clump"),
			tag("combine"),
			tag("complement"),
			tag("concat"),
			tag("difference"),
			tag("distinct"),
			tag("filter_index"),
			tag("find_index"),
			tag("first"),
			tag("flatten"),
			tag("group"),
			tag("insert"),
		)),
		alt((
			tag("intersect"),
			tag("join"),
			tag("last"),
			tag("len"),
			tag("logical_and"),
			tag("logical_or"),
			tag("logical_xor"),
			tag("matches"),
			tag("max"),
			tag("min"),
			tag("pop"),
			tag("prepend"),
			tag("push"),
		)),
		alt((
			tag("remove"),
			tag("reverse"),
			tag("slice"),
			tag("sort::asc"),
			tag("sort::desc"),
			tag("sort"),
			tag("transpose"),
			tag("union"),
		)),
	))(i)
}

fn function_bytes(i: &str) -> IResult<&str, &str> {
	alt((tag("len"),))(i)
}

fn function_crypto(i: &str) -> IResult<&str, &str> {
	alt((
		preceded(tag("argon2::"), alt((tag("compare"), tag("generate")))),
		preceded(tag("bcrypt::"), alt((tag("compare"), tag("generate")))),
		preceded(tag("pbkdf2::"), alt((tag("compare"), tag("generate")))),
		preceded(tag("scrypt::"), alt((tag("compare"), tag("generate")))),
		tag("md5"),
		tag("sha1"),
		tag("sha256"),
		tag("sha512"),
	))(i)
}

fn function_duration(i: &str) -> IResult<&str, &str> {
	alt((
		tag("days"),
		tag("hours"),
		tag("micros"),
		tag("millis"),
		tag("mins"),
		tag("nanos"),
		tag("secs"),
		tag("weeks"),
		tag("years"),
		preceded(
			tag("from::"),
			alt((
				tag("days"),
				tag("hours"),
				tag("micros"),
				tag("millis"),
				tag("mins"),
				tag("nanos"),
				tag("secs"),
				tag("weeks"),
			)),
		),
	))(i)
}

fn function_encoding(i: &str) -> IResult<&str, &str> {
	alt((preceded(tag("base64::"), alt((tag("decode"), tag("encode")))),))(i)
}

fn function_geo(i: &str) -> IResult<&str, &str> {
	alt((
		tag("area"),
		tag("bearing"),
		tag("centroid"),
		tag("distance"),
		preceded(tag("hash::"), alt((tag("decode"), tag("encode")))),
	))(i)
}

fn function_http(i: &str) -> IResult<&str, &str> {
	alt((tag("head"), tag("get"), tag("put"), tag("post"), tag("patch"), tag("delete")))(i)
}

fn function_is(i: &str) -> IResult<&str, &str> {
	alt((
		tag("alphanum"),
		tag("alpha"),
		tag("ascii"),
		tag("datetime"),
		tag("domain"),
		tag("email"),
		tag("hexadecimal"),
		tag("latitude"),
		tag("longitude"),
		tag("numeric"),
		tag("semver"),
		tag("url"),
		tag("uuid"),
	))(i)
}

fn function_math(i: &str) -> IResult<&str, &str> {
	alt((
		alt((
			tag("abs"),
			tag("bottom"),
			tag("ceil"),
			tag("fixed"),
			tag("floor"),
			tag("interquartile"),
			tag("max"),
			tag("mean"),
			tag("median"),
			tag("midhinge"),
			tag("min"),
			tag("mode"),
		)),
		alt((
			tag("nearestrank"),
			tag("percentile"),
			tag("pow"),
			tag("product"),
			tag("round"),
			tag("spread"),
			tag("sqrt"),
			tag("stddev"),
			tag("sum"),
			tag("top"),
			tag("trimean"),
			tag("variance"),
		)),
	))(i)
}

fn function_meta(i: &str) -> IResult<&str, &str> {
	alt((tag("id"), tag("table"), tag("tb")))(i)
}

fn function_parse(i: &str) -> IResult<&str, &str> {
	alt((
		preceded(tag("email::"), alt((tag("host"), tag("user")))),
		preceded(
			tag("url::"),
			alt((
				tag("domain"),
				tag("fragment"),
				tag("host"),
				tag("path"),
				tag("port"),
				tag("query"),
				tag("scheme"),
			)),
		),
	))(i)
}

fn function_rand(i: &str) -> IResult<&str, &str> {
	alt((
		tag("bool"),
		tag("enum"),
		tag("float"),
		tag("guid"),
		tag("int"),
		tag("string"),
		tag("time"),
		tag("ulid"),
		tag("uuid::v4"),
		tag("uuid::v7"),
		tag("uuid"),
	))(i)
}

fn function_search(i: &str) -> IResult<&str, &str> {
	alt((tag("score"), tag("highlight"), tag("offsets")))(i)
}

fn function_session(i: &str) -> IResult<&str, &str> {
	alt((
		tag("db"),
		tag("id"),
		tag("ip"),
		tag("ns"),
		tag("origin"),
		tag("sc"),
		tag("sd"),
		tag("token"),
	))(i)
}

fn function_string(i: &str) -> IResult<&str, &str> {
	alt((
		tag("concat"),
		tag("contains"),
		tag("endsWith"),
		tag("join"),
		tag("len"),
		tag("lowercase"),
		tag("repeat"),
		tag("replace"),
		tag("reverse"),
		tag("slice"),
		tag("slug"),
		tag("split"),
		tag("startsWith"),
		tag("trim"),
		tag("uppercase"),
		tag("words"),
		preceded(tag("distance::"), alt((tag("hamming"), tag("levenshtein")))),
		preceded(tag("similarity::"), alt((tag("fuzzy"), tag("jaro"), tag("smithwaterman")))),
	))(i)
}

fn function_time(i: &str) -> IResult<&str, &str> {
	alt((
		tag("ceil"),
		tag("day"),
		tag("floor"),
		tag("format"),
		tag("group"),
		tag("hour"),
		tag("minute"),
		tag("max"),
		tag("min"),
		tag("month"),
		tag("nano"),
		tag("now"),
		tag("round"),
		tag("second"),
		tag("timezone"),
		tag("unix"),
		tag("wday"),
		tag("week"),
		tag("yday"),
		tag("year"),
		preceded(tag("from::"), alt((tag("micros"), tag("millis"), tag("secs"), tag("unix")))),
	))(i)
}

fn function_type(i: &str) -> IResult<&str, &str> {
	alt((
		tag("bool"),
		tag("datetime"),
		tag("decimal"),
		tag("duration"),
		tag("fields"),
		tag("field"),
		tag("float"),
		tag("int"),
		tag("number"),
		tag("point"),
		tag("string"),
		tag("table"),
		tag("thing"),
	))(i)
}

fn function_vector(i: &str) -> IResult<&str, &str> {
	alt((
		tag("add"),
		tag("angle"),
		tag("divide"),
		tag("cross"),
		tag("dot"),
		tag("magnitude"),
		tag("multiply"),
		tag("normalize"),
		tag("project"),
		tag("subtract"),
		preceded(
			tag("distance::"),
			alt((
				tag("chebyshev"),
				tag("euclidean"),
				tag("hamming"),
				tag("mahalanobis"),
				tag("manhattan"),
				tag("minkowski"),
			)),
		),
		preceded(
			tag("similarity::"),
			alt((tag("cosine"), tag("jaccard"), tag("pearson"), tag("spearman"))),
		),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

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
		let sql = "is::numeric(null)";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("is::numeric(NULL)", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("is::numeric"), vec![Value::Null]));
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
