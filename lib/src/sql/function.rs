use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::script::{script, Script};
use crate::sql::value::{single, value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::multi::separated_list0;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Function {
	Future(Value),
	Script(Script),
	Cast(String, Value),
	Normal(String, Vec<Value>),
}

impl PartialOrd for Function {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		unreachable!()
	}
}

impl Function {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self {
			Function::Future(ref e) => match opt.futures {
				true => {
					let a = e.compute(ctx, opt, txn, doc).await?;
					fnc::future::run(ctx, a)
				}
				false => Ok(self.to_owned().into()),
			},
			Function::Script(ref s) => {
				let a = s.to_owned();
				fnc::script::run(ctx, a)
			}
			Function::Cast(ref s, ref e) => {
				let a = e.compute(ctx, opt, txn, doc).await?;
				fnc::cast::run(ctx, s, a)
			}
			Function::Normal(ref s, ref e) => {
				let mut a: Vec<Value> = vec![];
				for v in e {
					let v = v.compute(ctx, opt, txn, doc).await?;
					a.push(v);
				}
				fnc::run(ctx, s, a).await
			}
		}
	}
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Function::Future(ref e) => write!(f, "fn::future -> {{ {} }}", e),
			Function::Script(ref s) => write!(f, "fn::script -> {{ {} }}", s),
			Function::Cast(ref s, ref e) => write!(f, "<{}> {}", s, e),
			Function::Normal(ref s, ref e) => write!(
				f,
				"{}({})",
				s,
				e.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
			),
		}
	}
}

pub fn function(i: &str) -> IResult<&str, Function> {
	alt((casts, langs, future, normal))(i)
}

fn future(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn::future")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("->")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("{")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = value(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("}")(i)?;
	Ok((i, Function::Future(v)))
}

fn langs(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn::script")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("->")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("{")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = script(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("}")(i)?;
	Ok((i, Function::Script(v)))
}

fn casts(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("<")(i)?;
	let (i, s) = function_casts(i)?;
	let (i, _) = tag(">")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((i, Function::Cast(s.to_string(), v)))
}

fn normal(i: &str) -> IResult<&str, Function> {
	let (i, s) = function_names(i)?;
	let (i, _) = tag("(")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, value)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(")")(i)?;
	Ok((i, Function::Normal(s.to_string(), v)))
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
		function_geo,
		function_http,
		function_is,
		function_math,
		function_parse,
		function_rand,
		function_string,
		function_time,
		function_type,
	))(i)
}

fn function_array(i: &str) -> IResult<&str, &str> {
	alt((
		tag("array::combine"),
		tag("array::concat"),
		tag("array::difference"),
		tag("array::distinct"),
		tag("array::intersect"),
		tag("array::len"),
		tag("array::union"),
	))(i)
}

fn function_count(i: &str) -> IResult<&str, &str> {
	tag("count")(i)
}

fn function_crypto(i: &str) -> IResult<&str, &str> {
	alt((
		tag("crypto::md5"),
		tag("crypto::sha1"),
		tag("crypto::sha256"),
		tag("crypto::sha512"),
		tag("crypto::argon2::compare"),
		tag("crypto::argon2::generate"),
		tag("crypto::pbkdf2::compare"),
		tag("crypto::pbkdf2::generate"),
		tag("crypto::scrypt::compare"),
		tag("crypto::scrypt::generate"),
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
		tag("is::domain"),
		tag("is::email"),
		tag("is::hexadecimal"),
		tag("is::latitude"),
		tag("is::longitude"),
		tag("is::numeric"),
		tag("is::semver"),
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

fn function_parse(i: &str) -> IResult<&str, &str> {
	alt((
		tag("parse::email::domain"),
		tag("parse::email::user"),
		tag("parse::url::domain"),
		tag("parse::url::fragment"),
		tag("parse::url::host"),
		tag("parse::url::port"),
		tag("parse::url::path"),
		tag("parse::url::query"),
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
		tag("rand::uuid"),
		tag("rand"),
	))(i)
}

fn function_string(i: &str) -> IResult<&str, &str> {
	alt((
		tag("string::concat"),
		tag("string::contains"),
		tag("string::endsWith"),
		tag("string::join"),
		tag("string::length"),
		tag("string::lowercase"),
		tag("string::repeat"),
		tag("string::replace"),
		tag("string::reverse"),
		tag("string::slice"),
		tag("string::slug"),
		tag("string::split"),
		tag("string::startsWith"),
		tag("string::substr"),
		tag("string::trim"),
		tag("string::uppercase"),
		tag("string::words"),
	))(i)
}

fn function_time(i: &str) -> IResult<&str, &str> {
	alt((
		tag("time::add"),
		tag("time::age"),
		tag("time::day"),
		tag("time::floor"),
		tag("time::group"),
		tag("time::hour"),
		tag("time::mins"),
		tag("time::month"),
		tag("time::nano"),
		tag("time::now"),
		tag("time::round"),
		tag("time::secs"),
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
	use crate::sql::expression::Expression;
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
	fn function_future_expression() {
		let sql = "fn::future -> { 1.2345 + 5.4321 }";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("fn::future -> { 1.2345 + 5.4321 }", format!("{}", out));
		assert_eq!(out, Function::Future(Value::from(Expression::parse("1.2345 + 5.4321"))));
	}

	#[test]
	fn function_script_expression() {
		let sql = "fn::script -> { 1.2345 + 5.4321 }";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"fn::script -> { return this.tags.filter(t => { return t.length > 3; }); }",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Function::Script(Script::from(
				"return this.tags.filter(t => { return t.length > 3; });"
			))
		);
	}
}
