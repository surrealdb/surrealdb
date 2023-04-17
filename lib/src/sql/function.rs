use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::common::val_char;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::script::{script as func, Script};
use crate::sql::serde::is_internal_serialization;
use crate::sql::value::{single, value, Value};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::recognize;
use nom::multi::separated_list0;
use nom::multi::separated_list1;
use nom::sequence::delimited;
use nom::sequence::preceded;
use serde::ser::SerializeTupleVariant;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Hash)]
pub enum Function {
	Cast(String, Value),
	Normal(String, Vec<Value>),
	Custom(String, Vec<Value>),
	Script(Script, Vec<Value>),
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
			Self::Cast(_, v) => v.to_idiom(),
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
	/// Check if this function is a rolling function
	pub fn is_rolling(&self) -> bool {
		match self {
			Self::Normal(f, _) if f == "count" => true,
			Self::Normal(f, _) if f == "math::max" => true,
			Self::Normal(f, _) if f == "math::mean" => true,
			Self::Normal(f, _) if f == "math::min" => true,
			Self::Normal(f, _) if f == "math::sum" => true,
			_ => false,
		}
	}
	/// Check if this function is a grouping function
	pub fn is_aggregate(&self) -> bool {
		match self {
			Self::Normal(f, _) if f == "array::distinct" => true,
			Self::Normal(f, _) if f == "array::group" => true,
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
			_ => false,
		}
	}
}

impl Function {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion Value>,
	) -> Result<Value, Error> {
		// Prevent long function chains
		let opt = &opt.dive(1)?;
		// Ensure futures are run
		let opt = &opt.futures(true);
		// Process the function type
		match self {
			Self::Cast(s, x) => {
				// Compute the value to be cast
				let a = x.compute(ctx, opt, txn, doc).await?;
				// Run the cast function
				fnc::cast::run(ctx, s, a)
			}
			Self::Normal(s, x) => {
				// Compute the function arguments
				let a = try_join_all(x.iter().map(|v| v.compute(ctx, opt, txn, doc))).await?;
				// Run the normal function
				fnc::run(ctx, s, a).await
			}
			Self::Custom(s, x) => {
				// Get the function definition
				let val = {
					// Clone transaction
					let run = txn.clone();
					// Claim transaction
					let mut run = run.lock().await;
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
					ctx.add_value(
						name.to_raw(),
						match val {
							Value::None => val,
							Value::Null => val,
							_ => val.convert_to(&kind),
						},
					);
				}
				// Run the custom function
				val.block.compute(&ctx, opt, txn, doc).await
			}
			#[allow(unused_variables)]
			Self::Script(s, x) => {
				#[cfg(feature = "scripting")]
				{
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
			Self::Cast(s, e) => write!(f, "<{s}> {e}"),
			Self::Normal(s, e) => write!(f, "{s}({})", Fmt::comma_separated(e)),
			Self::Custom(s, e) => write!(f, "fn::{s}({})", Fmt::comma_separated(e)),
			Self::Script(s, e) => write!(f, "function({}) {{{s}}}", Fmt::comma_separated(e)),
		}
	}
}

impl Serialize for Function {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			match self {
				Self::Cast(s, e) => {
					let mut serializer = serializer.serialize_tuple_variant(TOKEN, 0, "Cast", 2)?;
					serializer.serialize_field(s)?;
					serializer.serialize_field(e)?;
					serializer.end()
				}
				Self::Normal(s, e) => {
					let mut serializer =
						serializer.serialize_tuple_variant(TOKEN, 1, "Normal", 2)?;
					serializer.serialize_field(s)?;
					serializer.serialize_field(e)?;
					serializer.end()
				}
				Self::Custom(s, e) => {
					let mut serializer =
						serializer.serialize_tuple_variant(TOKEN, 2, "Custom", 2)?;
					serializer.serialize_field(s)?;
					serializer.serialize_field(e)?;
					serializer.end()
				}
				Self::Script(s, e) => {
					let mut serializer =
						serializer.serialize_tuple_variant(TOKEN, 3, "Script", 2)?;
					serializer.serialize_field(s)?;
					serializer.serialize_field(e)?;
					serializer.end()
				}
			}
		} else {
			serializer.serialize_none()
		}
	}
}

pub fn function(i: &str) -> IResult<&str, Function> {
	alt((normal, custom, script, cast))(i)
}

pub fn normal(i: &str) -> IResult<&str, Function> {
	let (i, s) = function_names(i)?;
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, a) = separated_list0(commas, value)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, Function::Normal(s.to_string(), a)))
}

pub fn custom(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn::")(i)?;
	let (i, s) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, a) = separated_list0(commas, value)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, Function::Custom(s.to_string(), a)))
}

fn script(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("function")(i)?;
	let (i, _) = tag("(")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, a) = separated_list0(commas, value)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(")")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('{')(i)?;
	let (i, v) = func(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, Function::Script(v, a)))
}

fn cast(i: &str) -> IResult<&str, Function> {
	let (i, s) = delimited(
		char('<'),
		alt((
			tag("bool"),
			tag("datetime"),
			tag("decimal"),
			tag("duration"),
			tag("float"),
			tag("int"),
			tag("number"),
			tag("string"),
		)),
		char('>'),
	)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((i, Function::Cast(s.to_string(), v)))
}

fn function_names(i: &str) -> IResult<&str, &str> {
	recognize(alt((
		preceded(tag("array::"), function_array),
		preceded(tag("crypto::"), function_crypto),
		preceded(tag("duration::"), function_duration),
		preceded(tag("geo::"), function_geo),
		preceded(tag("http::"), function_http),
		preceded(tag("is::"), function_is),
		preceded(tag("math::"), function_math),
		preceded(tag("meta::"), function_meta),
		preceded(tag("parse::"), function_parse),
		preceded(tag("rand::"), function_rand),
		preceded(tag("session::"), function_session),
		preceded(tag("string::"), function_string),
		preceded(tag("time::"), function_time),
		preceded(tag("type::"), function_type),
		tag("count"),
		tag("not"),
		tag("rand"),
		tag("sleep"),
	)))(i)
}

fn function_array(i: &str) -> IResult<&str, &str> {
	alt((
		alt((
			tag("add"),
			tag("all"),
			tag("any"),
			tag("append"),
			tag("combine"),
			tag("complement"),
			tag("concat"),
			tag("difference"),
			tag("distinct"),
			tag("flatten"),
			tag("group"),
			tag("insert"),
		)),
		alt((
			tag("intersect"),
			tag("len"),
			tag("max"),
			tag("min"),
			tag("pop"),
			tag("prepend"),
			tag("push"),
			tag("remove"),
			tag("reverse"),
			tag("sort::asc"),
			tag("sort::desc"),
			tag("sort"),
			tag("union"),
		)),
	))(i)
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
	alt((tag("days"), tag("hours"), tag("mins"), tag("secs"), tag("weeks"), tag("years")))(i)
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
	))(i)
}

fn function_time(i: &str) -> IResult<&str, &str> {
	alt((
		tag("day"),
		tag("floor"),
		tag("format"),
		tag("group"),
		tag("hour"),
		tag("minute"),
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
	))(i)
}

fn function_type(i: &str) -> IResult<&str, &str> {
	alt((
		tag("bool"),
		tag("datetime"),
		tag("decimal"),
		tag("duration"),
		tag("float"),
		tag("int"),
		tag("number"),
		tag("point"),
		tag("regex"),
		tag("string"),
		tag("table"),
		tag("thing"),
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
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("count()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("count"), vec![]));
	}

	#[test]
	fn function_single_not() {
		let sql = "not(1.2345)";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("not(1.2345)", format!("{}", out));
		assert_eq!(out, Function::Normal("not".to_owned(), vec![1.2345.into()]));
	}

	#[test]
	fn function_module() {
		let sql = "rand::uuid()";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("rand::uuid()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("rand::uuid"), vec![]));
	}

	#[test]
	fn function_arguments() {
		let sql = "is::numeric(null)";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("is::numeric(NULL)", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("is::numeric"), vec![Value::Null]));
	}

	#[test]
	fn function_casting_number() {
		let sql = "<int>1.2345";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<int> 1.2345", format!("{}", out));
		assert_eq!(out, Function::Cast(String::from("int"), 1.2345.into()));
	}

	#[test]
	fn function_casting_string() {
		let sql = "<string>1.2345";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<string> 1.2345", format!("{}", out));
		assert_eq!(out, Function::Cast(String::from("string"), 1.2345.into()));
	}

	#[test]
	fn function_script_expression() {
		let sql = "function() { return this.tags.filter(t => { return t.length > 3; }); }";
		let res = function(sql);
		assert!(res.is_ok());
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
