use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;
use once_cell::sync::Lazy;
use regex::Regex;
use std::char;

#[rustfmt::skip] static UUID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$").unwrap());
#[rustfmt::skip] static USER_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(?i)[a-z0-9.!#$%&'*+/=?^_`{|}~-]+\z").unwrap());
#[rustfmt::skip] static HOST_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$").unwrap());
#[rustfmt::skip] static DOMAIN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^([a-zA-Z0-9_]{1}[a-zA-Z0-9_-]{0,62}){1}(\.[a-zA-Z0-9_]{1}[a-zA-Z0-9_-]{0,62})*[\._]?$",).unwrap());
#[rustfmt::skip] static SEMVER_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^v?(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)(-(0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(\\.(0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*)?(\\+[0-9a-zA-Z-]+(\\.[0-9a-zA-Z-]+)*)?$").unwrap());
#[rustfmt::skip] static LATITUDE_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());
#[rustfmt::skip] static LONGITUDE_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());

pub fn alphanum(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(char::is_alphanumeric).into())
}

pub fn alpha(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(char::is_alphabetic).into())
}

pub fn ascii(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().is_ascii().into())
}

pub fn domain(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(DOMAIN_RE.is_match(args.remove(0).as_string().as_str()).into())
}

pub fn email(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args
		.remove(0)
		.as_string()
		.rsplit_once('@')
		.map(|(user, host)| USER_RE.is_match(user) && HOST_RE.is_match(host))
		.unwrap_or(false)
		.into())
}

pub fn hexadecimal(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(|x| char::is_ascii_hexdigit(&x)).into())
}

pub fn latitude(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(LATITUDE_RE.is_match(args.remove(0).as_string().as_str()).into())
}

pub fn longitude(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(LONGITUDE_RE.is_match(args.remove(0).as_string().as_str()).into())
}

pub fn numeric(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(char::is_numeric).into())
}

pub fn semver(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(SEMVER_RE.is_match(args.remove(0).as_string().as_str()).into())
}

pub fn uuid(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(UUID_RE.is_match(args.remove(0).as_string().as_str()).into())
}
