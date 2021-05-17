use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::doc::Document;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::expression::{expression, Expression};
use crate::sql::literal::Literal;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::multi::separated_list0;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Function {
	Future(Expression),
	Cast(String, Expression),
	Normal(String, Vec<Expression>),
}

impl PartialOrd for Function {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		unreachable!()
	}
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Function::Future(ref e) => write!(f, "fn() -> {{ {} }}", e),
			Function::Cast(ref s, ref e) => write!(f, "<{}>{}", s, e),
			Function::Normal(ref s, ref e) => write!(
				f,
				"{}({})",
				s,
				e.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
			),
		}
	}
}

impl dbs::Process for Function {
	fn process(
		&self,
		ctx: &Runtime,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		match self {
			Function::Future(ref e) => {
				let a = e.process(ctx, exe, doc)?;
				fnc::future::run(ctx, a)
			}
			Function::Cast(ref s, ref e) => {
				let a = e.process(ctx, exe, doc)?;
				fnc::cast::run(ctx, s, a)
			}
			Function::Normal(ref s, ref e) => {
				let mut a: Vec<Literal> = vec![];
				for v in e {
					let v = v.process(ctx, exe, doc)?;
					a.push(v);
				}
				fnc::run(ctx, s, a)
			}
		}
	}
}

pub fn function(i: &str) -> IResult<&str, Function> {
	alt((casts, future, normal))(i)
}

fn future(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn()")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("->")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("{")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = expression(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("}")(i)?;
	Ok((i, Function::Future(v)))
}

fn casts(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("<")(i)?;
	let (i, s) = function_casts(i)?;
	let (i, _) = tag(">")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = expression(i)?;
	Ok((i, Function::Cast(s.to_string(), v)))
}

fn normal(i: &str) -> IResult<&str, Function> {
	let (i, s) = function_names(i)?;
	let (i, _) = tag("(")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, expression)(i)?;
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
		function_geo,
		function_hash,
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
		tag("array::difference"),
		tag("array::distinct"),
		tag("array::intersect"),
		tag("array::union"),
	))(i)
}

fn function_count(i: &str) -> IResult<&str, &str> {
	alt((
		tag("count::all"),
		tag("count::if"),
		tag("count::not"),
		tag("count::oneof"),
		tag("count::between"),
		tag("count"),
	))(i)
}

fn function_geo(i: &str) -> IResult<&str, &str> {
	alt((
		tag("geo::area"),
		tag("geo::bearing"),
		tag("geo::center"),
		tag("geo::centroid"),
		tag("geo::circle"),
		tag("geo::distance"),
		tag("geo::latitude"),
		tag("geo::longitude"),
		tag("geo::midpoint"),
		tag("geo::hash::decode"),
		tag("geo::hash::encode"),
	))(i)
}

fn function_hash(i: &str) -> IResult<&str, &str> {
	alt((
		tag("hash::md5"),
		tag("hash::sha1"),
		tag("hash::sha256"),
		tag("hash::sha512"),
		tag("hash::bcrypt::compare"),
		tag("hash::bcrypt::generate"),
		tag("hash::scrypt::compare"),
		tag("hash::scrypt::generate"),
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
		tag("http::async::head"),
		tag("http::async::get"),
		tag("http::async::put"),
		tag("http::async::post"),
		tag("http::async::patch"),
		tag("http::async::delete"),
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
			tag("math::correlation"),
			tag("math::count"),
			tag("math::covariance"),
			tag("math::fixed"),
			tag("math::floor"),
			tag("math::geometricmean"),
			tag("math::harmonicmean"),
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
			tag("math::round"),
			tag("math::sample"),
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
		tag("parse::url::host"),
		tag("parse::url::port"),
		tag("parse::url::path"),
	))(i)
}

fn function_rand(i: &str) -> IResult<&str, &str> {
	alt((
		alt((
			tag("guid"),
			tag("uuid"),
			tag("rand::bool"),
			tag("rand::guid"),
			tag("rand::uuid"),
			tag("rand::enum"),
			tag("rand::time"),
			tag("rand::string"),
			tag("rand::integer"),
			tag("rand::decimal"),
			tag("rand::sentence"),
			tag("rand::paragraph"),
		)),
		alt((
			tag("rand::person::email"),
			tag("rand::person::phone"),
			tag("rand::person::fullname"),
			tag("rand::person::firstname"),
			tag("rand::person::lastname"),
			tag("rand::person::username"),
			tag("rand::person::jobtitle"),
		)),
		alt((
			tag("rand::location::name"),
			tag("rand::location::address"),
			tag("rand::location::street"),
			tag("rand::location::city"),
			tag("rand::location::state"),
			tag("rand::location::county"),
			tag("rand::location::zipcode"),
			tag("rand::location::postcode"),
			tag("rand::location::country"),
			tag("rand::location::altitude"),
			tag("rand::location::latitude"),
			tag("rand::location::longitude"),
		)),
		tag("rand"),
	))(i)
}

fn function_string(i: &str) -> IResult<&str, &str> {
	alt((
		tag("string::concat"),
		tag("string::contains"),
		tag("string::endsWith"),
		tag("string::format"),
		tag("string::includes"),
		tag("string::join"),
		tag("string::length"),
		tag("string::lowercase"),
		tag("string::repeat"),
		tag("string::replace"),
		tag("string::reverse"),
		tag("string::search"),
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
		tag("time::now"),
		tag("time::add"),
		tag("time::age"),
		tag("time::floor"),
		tag("time::round"),
		tag("time::day"),
		tag("time::hour"),
		tag("time::mins"),
		tag("time::month"),
		tag("time::nano"),
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
		tag("type::batch"),
		tag("type::model"),
		tag("type::point"),
		tag("type::polygon"),
		tag("type::regex"),
		tag("type::table"),
		tag("type::thing"),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

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
		let sql = "count::if()";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("count::if()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("count::if"), vec![]));
	}

	#[test]
	fn function_arguments() {
		let sql = "is::numeric(null)";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("is::numeric(NULL)", format!("{}", out));
		assert_eq!(
			out,
			Function::Normal(String::from("is::numeric"), vec![Expression::from("null")])
		);
	}

	#[test]
	fn function_casting_number() {
		let sql = "<int>1.2345";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<int>1.2345", format!("{}", out));
		assert_eq!(out, Function::Cast(String::from("int"), Expression::from("1.2345")));
	}

	#[test]
	fn function_casting_string() {
		let sql = "<string>1.2345";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<string>1.2345", format!("{}", out));
		assert_eq!(out, Function::Cast(String::from("string"), Expression::from("1.2345")));
	}

	#[test]
	fn function_future_expression() {
		let sql = "fn() -> { 1.2345 + 5.4321 }";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("fn() -> { 1.2345 + 5.4321 }", format!("{}", out));
		assert_eq!(out, Function::Future(Expression::from("1.2345 + 5.4321")));
	}
}
