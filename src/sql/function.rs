use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::expression::{expression, Expression};
use crate::sql::literal::simple;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::multi::separated_list;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Function {
	pub name: String,
	pub args: Vec<Expression>,
	pub cast: bool,
	pub func: bool,
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.func {
			return write!(
				f,
				"{}() -> {{ {} }}",
				self.name,
				self.args.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "),
			);
		}
		if self.cast {
			return write!(
				f,
				"<{}>{}",
				self.name,
				self.args.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "),
			);
		}
		write!(
			f,
			"{}({})",
			self.name,
			self.args.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "),
		)
	}
}

pub fn function(i: &str) -> IResult<&str, Function> {
	alt((
		casts,
		future,
		function_all,
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

fn casts(i: &str) -> IResult<&str, Function> {
	alt((
		cast("bool"),
		cast("int16"),
		cast("int32"),
		cast("int64"),
		cast("int128"),
		cast("uint16"),
		cast("uint32"),
		cast("uint64"),
		cast("uint128"),
		cast("float32"),
		cast("float64"),
		cast("decimal"),
		cast("number"),
		cast("string"),
		cast("binary"),
		cast("bytes"),
		cast("datetime"),
		cast("duration"),
	))(i)
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
	Ok((
		i,
		Function {
			name: String::from("fn"),
			args: vec![v],
			cast: false,
			func: true,
		},
	))
}

fn function_all(i: &str) -> IResult<&str, Function> {
	alt((func("if"), func("either")))(i)
}

fn function_array(i: &str) -> IResult<&str, Function> {
	alt((
		func("array::difference"),
		func("array::distinct"),
		func("array::intersect"),
		func("array::union"),
	))(i)
}

fn function_count(i: &str) -> IResult<&str, Function> {
	alt((func("count"), func("count::if"), func("count::not")))(i)
}

fn function_geo(i: &str) -> IResult<&str, Function> {
	alt((
		func("geo::circle"),
		func("geo::distance"),
		func("geo::point"),
		func("geo::polygon"),
		func("geo::hash::decode"),
		func("geo::hash::encode"),
	))(i)
}

fn function_hash(i: &str) -> IResult<&str, Function> {
	alt((
		func("hash::md5"),
		func("hash::sha1"),
		func("hash::sha256"),
		func("hash::sha512"),
		func("hash::bcrypt"),
		func("hash::bcrypt::compare"),
		func("hash::bcrypt::generate"),
		func("hash::scrypt"),
		func("hash::scrypt::compare"),
		func("hash::scrypt::generate"),
	))(i)
}

fn function_http(i: &str) -> IResult<&str, Function> {
	alt((
		func("http::head"),
		func("http::get"),
		func("http::put"),
		func("http::post"),
		func("http::patch"),
		func("http::delete"),
		func("http::async::head"),
		func("http::async::get"),
		func("http::async::put"),
		func("http::async::post"),
		func("http::async::patch"),
		func("http::async::delete"),
	))(i)
}

fn function_is(i: &str) -> IResult<&str, Function> {
	alt((
		func("is::alpha"),
		func("is::alphanum"),
		func("is::ascii"),
		func("is::domain"),
		func("is::email"),
		func("is::hexadecimal"),
		func("is::latitude"),
		func("is::longitude"),
		func("is::numeric"),
		func("is::semver"),
		func("is::uuid"),
	))(i)
}

fn function_math(i: &str) -> IResult<&str, Function> {
	alt((
		alt((
			func("math::abs"),
			func("math::bottom"),
			func("math::ceil"),
			func("math::correlation"),
			func("math::count"),
			func("math::covariance"),
			func("math::fixed"),
			func("math::floor"),
			func("math::geometricmean"),
			func("math::harmonicmean"),
			func("math::interquartile"),
		)),
		alt((
			func("math::max"),
			func("math::mean"),
			func("math::median"),
			func("math::midhinge"),
			func("math::min"),
			func("math::mode"),
		)),
		alt((
			func("math::nearestrank"),
			func("math::percentile"),
			func("math::round"),
			func("math::sample"),
			func("math::spread"),
			func("math::sqrt"),
			func("math::stddev"),
			func("math::sum"),
			func("math::top"),
			func("math::trimean"),
			func("math::variance"),
		)),
	))(i)
}

fn function_parse(i: &str) -> IResult<&str, Function> {
	alt((
		func("parse::email::domain"),
		func("parse::email::user"),
		func("parse::url::domain"),
		func("parse::url::host"),
		func("parse::url::port"),
		func("parse::url::path"),
	))(i)
}

fn function_rand(i: &str) -> IResult<&str, Function> {
	alt((
		alt((
			func("rand"),
			func("guid"),
			func("uuid"),
			func("rand::bool"),
			func("rand::guid"),
			func("rand::uuid"),
			func("rand::enum"),
			func("rand::time"),
			func("rand::string"),
			func("rand::integer"),
			func("rand::decimal"),
			func("rand::sentence"),
			func("rand::paragraph"),
		)),
		alt((
			func("rand::person::email"),
			func("rand::person::phone"),
			func("rand::person::fullname"),
			func("rand::person::firstname"),
			func("rand::person::lastname"),
			func("rand::person::username"),
			func("rand::person::jobtitle"),
		)),
		alt((
			func("rand::location::name"),
			func("rand::location::address"),
			func("rand::location::street"),
			func("rand::location::city"),
			func("rand::location::state"),
			func("rand::location::county"),
			func("rand::location::zipcode"),
			func("rand::location::postcode"),
			func("rand::location::country"),
			func("rand::location::altitude"),
			func("rand::location::latitude"),
			func("rand::location::longitude"),
		)),
	))(i)
}

fn function_string(i: &str) -> IResult<&str, Function> {
	alt((
		func("string::concat"),
		func("string::contains"),
		func("string::endsWith"),
		func("string::format"),
		func("string::includes"),
		func("string::join"),
		func("string::length"),
		func("string::lowercase"),
		func("string::repeat"),
		func("string::replace"),
		func("string::reverse"),
		func("string::search"),
		func("string::slice"),
		func("string::slug"),
		func("string::split"),
		func("string::startsWith"),
		func("string::substr"),
		func("string::trim"),
		func("string::uppercase"),
		func("string::words"),
	))(i)
}

fn function_time(i: &str) -> IResult<&str, Function> {
	alt((
		func("time::now"),
		func("time::add"),
		func("time::age"),
		func("time::floor"),
		func("time::round"),
		func("time::day"),
		func("time::hour"),
		func("time::mins"),
		func("time::month"),
		func("time::nano"),
		func("time::secs"),
		func("time::unix"),
		func("time::wday"),
		func("time::week"),
		func("time::yday"),
		func("time::year"),
	))(i)
}

fn function_type(i: &str) -> IResult<&str, Function> {
	alt((
		func("type::batch"),
		func("type::model"),
		func("type::regex"),
		func("type::table"),
		func("type::thing"),
	))(i)
}

fn cast<'b, 'a: 'b>(f: &'a str) -> impl Fn(&'b str) -> IResult<&'b str, Function> where {
	move |i: &'b str| {
		let (i, _) = tag("<")(i)?;
		let (i, n) = tag(f)(i)?;
		let (i, _) = tag(">")(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, a) = simple(i)?;
		Ok((
			i,
			Function {
				name: n.to_string(),
				args: vec![Expression::from(a)],
				cast: true,
				func: false,
			},
		))
	}
}

fn func<'b, 'a: 'b>(f: &'a str) -> impl Fn(&'b str) -> IResult<&'b str, Function> where {
	move |i: &'b str| {
		let (i, n) = tag(f)(i)?;
		let (i, _) = tag("(")(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = separated_list(commas, expression)(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = tag(")")(i)?;
		Ok((
			i,
			Function {
				name: n.to_string(),
				args: v,
				cast: false,
				func: false,
			},
		))
	}
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
		assert_eq!(
			out,
			Function {
				name: String::from("count"),
				args: vec![],
				cast: false,
				func: false,
			}
		);
	}

	#[test]
	fn function_module() {
		let sql = "count::if()";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("count::if()", format!("{}", out));
		assert_eq!(
			out,
			Function {
				name: String::from("count::if"),
				args: vec![],
				cast: false,
				func: false,
			}
		);
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
			Function {
				name: String::from("is::numeric"),
				args: vec![Expression::from("null")],
				cast: false,
				func: false,
			}
		);
	}

	#[test]
	fn function_casting_number() {
		let sql = "<uint64>1.2345";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<uint64>1.2345", format!("{}", out));
		assert_eq!(
			out,
			Function {
				name: String::from("uint64"),
				args: vec![Expression::from("1.2345")],
				cast: true,
				func: false,
			}
		);
	}

	#[test]
	fn function_casting_string() {
		let sql = "<string>1.2345";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<string>1.2345", format!("{}", out));
		assert_eq!(
			out,
			Function {
				name: String::from("string"),
				args: vec![Expression::from("1.2345")],
				cast: true,
				func: false,
			}
		);
	}

	#[test]
	fn function_future_expression() {
		let sql = "fn() -> { 1.2345 + 5.4321 }";
		let res = function(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("fn() -> { 1.2345 + 5.4321 }", format!("{}", out));
		assert_eq!(
			out,
			Function {
				name: String::from("fn"),
				args: vec![Expression::from("1.2345 + 5.4321")],
				cast: false,
				func: true,
			}
		);
	}
}
