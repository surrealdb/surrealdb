use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::script::{script as func, Script};
use crate::sql::value::{single, value, Value};
use futures::future::try_join_all;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::multi::separated_list0;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Function {
	Cast(String, Value),
	Normal(String, Vec<Value>),
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
			_ => unreachable!(),
		}
	}
	/// Get function arguments if applicable
	pub fn args(&self) -> &[Value] {
		match self {
			Self::Normal(_, a) => a,
			_ => &[],
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
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
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
			Self::Cast(ref s, ref e) => write!(f, "<{s}> {e}"),
			Self::Script(ref s, ref e) => {
				write!(f, "function({}) {{{s}}}", Fmt::comma_separated(e))
			}
			Self::Normal(ref s, ref e) => write!(f, "{s}({})", Fmt::comma_separated(e)),
		}
	}
}

pub fn function(i: &str) -> IResult<&str, Function> {
	alt((normal, script, cast))(i)
}

fn normal(i: &str) -> IResult<&str, Function> {
	let (i, s) = function_names(i)?;
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, a) = separated_list0(commas, value)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, Function::Normal(s.to_string(), a)))
}

fn script(i: &str) -> IResult<&str, Function> {
	let (i, _) = alt((tag("fn::script"), tag("fn"), tag("function")))(i)?;
	let (i, _) = mightbespace(i)?;
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
	let (i, _) = char('<')(i)?;
	let (i, s) = function_casts(i)?;
	let (i, _) = char('>')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((i, Function::Cast(s.to_string(), v)))
}

fn function_casts(i: &str) -> IResult<&str, &str> {
	alt((
		tag("bool"),
		tag("int"),
		tag("float"),
		tag("string"),
		tag("number"),
		tag("decimal"),
		tag("datetime"),
		tag("duration"),
	))(i)
}

fn function_names(i: &str) -> IResult<&str, &str> {
	alt((
		function_array,
		function_count,
		function_crypto,
		function_duration,
		function_geo,
		function_http,
		function_is,
		function_math,
		function_meta,
		function_not,
		function_parse,
		function_rand,
		function_session,
		function_sleep,
		function_string,
		function_time,
		function_type,
	))(i)
}

fn function_array(i: &str) -> IResult<&str, &str> {
	alt((
		tag("array::all"),
		tag("array::any"),
		tag("array::combine"),
		tag("array::complement"),
		tag("array::concat"),
		tag("array::difference"),
		tag("array::distinct"),
		tag("array::flatten"),
		tag("array::group"),
		tag("array::insert"),
		tag("array::intersect"),
		tag("array::len"),
		tag("array::max"),
		tag("array::min"),
		tag("array::sort::asc"),
		tag("array::sort::desc"),
		tag("array::sort"),
		tag("array::union"),
	))(i)
}

fn function_count(i: &str) -> IResult<&str, &str> {
	tag("count")(i)
}

fn function_crypto(i: &str) -> IResult<&str, &str> {
	alt((
		tag("crypto::argon2::compare"),
		tag("crypto::argon2::generate"),
		tag("crypto::bcrypt::compare"),
		tag("crypto::bcrypt::generate"),
		tag("crypto::md5"),
		tag("crypto::pbkdf2::compare"),
		tag("crypto::pbkdf2::generate"),
		tag("crypto::scrypt::compare"),
		tag("crypto::scrypt::generate"),
		tag("crypto::sha1"),
		tag("crypto::sha256"),
		tag("crypto::sha512"),
	))(i)
}

fn function_duration(i: &str) -> IResult<&str, &str> {
	alt((
		tag("duration::days"),
		tag("duration::hours"),
		tag("duration::mins"),
		tag("duration::secs"),
		tag("duration::weeks"),
		tag("duration::years"),
	))(i)
}

fn function_geo(i: &str) -> IResult<&str, &str> {
	alt((
		tag("geo::area"),
		tag("geo::bearing"),
		tag("geo::centroid"),
		tag("geo::distance"),
		tag("geo::hash::decode"),
		tag("geo::hash::encode"),
	))(i)
}

fn function_http(i: &str) -> IResult<&str, &str> {
	alt((
		tag("http::head"),
		tag("http::get"),
		tag("http::put"),
		tag("http::post"),
		tag("http::patch"),
		tag("http::delete"),
	))(i)
}

fn function_is(i: &str) -> IResult<&str, &str> {
	alt((
		tag("is::alphanum"),
		tag("is::alpha"),
		tag("is::ascii"),
		tag("is::datetime"),
		tag("is::domain"),
		tag("is::email"),
		tag("is::hexadecimal"),
		tag("is::latitude"),
		tag("is::longitude"),
		tag("is::numeric"),
		tag("is::semver"),
		tag("is::url"),
		tag("is::uuid"),
	))(i)
}

fn function_math(i: &str) -> IResult<&str, &str> {
	alt((
		alt((
			tag("math::abs"),
			tag("math::bottom"),
			tag("math::ceil"),
			tag("math::fixed"),
			tag("math::floor"),
			tag("math::interquartile"),
		)),
		alt((
			tag("math::max"),
			tag("math::mean"),
			tag("math::median"),
			tag("math::midhinge"),
			tag("math::min"),
			tag("math::mode"),
		)),
		alt((
			tag("math::nearestrank"),
			tag("math::percentile"),
			tag("math::pow"),
			tag("math::product"),
			tag("math::round"),
			tag("math::spread"),
			tag("math::sqrt"),
			tag("math::stddev"),
			tag("math::sum"),
			tag("math::top"),
			tag("math::trimean"),
			tag("math::variance"),
		)),
	))(i)
}

fn function_meta(i: &str) -> IResult<&str, &str> {
	alt((tag("meta::id"), tag("meta::table"), tag("meta::tb")))(i)
}

fn function_not(i: &str) -> IResult<&str, &str> {
	tag("not")(i)
}

fn function_sleep(i: &str) -> IResult<&str, &str> {
	tag("sleep")(i)
}

fn function_parse(i: &str) -> IResult<&str, &str> {
	alt((
		tag("parse::email::host"),
		tag("parse::email::user"),
		tag("parse::url::domain"),
		tag("parse::url::fragment"),
		tag("parse::url::host"),
		tag("parse::url::path"),
		tag("parse::url::port"),
		tag("parse::url::query"),
		tag("parse::url::scheme"),
	))(i)
}

fn function_rand(i: &str) -> IResult<&str, &str> {
	alt((
		tag("rand::bool"),
		tag("rand::enum"),
		tag("rand::float"),
		tag("rand::guid"),
		tag("rand::int"),
		tag("rand::string"),
		tag("rand::time"),
		tag("rand::ulid"),
		tag("rand::uuid::v4"),
		tag("rand::uuid::v7"),
		tag("rand::uuid"),
		tag("rand"),
	))(i)
}

fn function_session(i: &str) -> IResult<&str, &str> {
	alt((
		tag("session::db"),
		tag("session::id"),
		tag("session::ip"),
		tag("session::ns"),
		tag("session::origin"),
		tag("session::sc"),
		tag("session::sd"),
		tag("session::token"),
	))(i)
}

fn function_string(i: &str) -> IResult<&str, &str> {
	alt((
		tag("string::concat"),
		tag("string::endsWith"),
		tag("string::join"),
		tag("string::len"),
		tag("string::lowercase"),
		tag("string::repeat"),
		tag("string::replace"),
		tag("string::reverse"),
		tag("string::slice"),
		tag("string::slug"),
		tag("string::split"),
		tag("string::startsWith"),
		tag("string::trim"),
		tag("string::uppercase"),
		tag("string::words"),
	))(i)
}

fn function_time(i: &str) -> IResult<&str, &str> {
	alt((
		tag("time::day"),
		tag("time::floor"),
		tag("time::format"),
		tag("time::group"),
		tag("time::hour"),
		tag("time::minute"),
		tag("time::month"),
		tag("time::nano"),
		tag("time::now"),
		tag("time::round"),
		tag("time::second"),
		tag("time::timezone"),
		tag("time::unix"),
		tag("time::wday"),
		tag("time::week"),
		tag("time::yday"),
		tag("time::year"),
	))(i)
}

fn function_type(i: &str) -> IResult<&str, &str> {
	alt((
		tag("type::bool"),
		tag("type::datetime"),
		tag("type::decimal"),
		tag("type::duration"),
		tag("type::float"),
		tag("type::int"),
		tag("type::number"),
		tag("type::point"),
		tag("type::regex"),
		tag("type::string"),
		tag("type::table"),
		tag("type::thing"),
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
