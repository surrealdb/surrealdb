use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::util::string;
use crate::sql::value::Value;

pub fn concat(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.into_iter().map(|x| x.as_string()).collect::<Vec<_>>().concat().into())
}

pub fn ends_with(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let chr = args.remove(0).as_string();
	Ok(val.ends_with(&chr).into())
}

pub fn join(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let chr = args.remove(0).as_string();
	let val = args.into_iter().map(|x| x.as_string());
	let val = val.collect::<Vec<_>>().join(&chr);
	Ok(val.into())
}

pub fn length(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let num = val.chars().count() as i64;
	Ok(num.into())
}

pub fn lowercase(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().to_lowercase().into())
}

pub fn repeat(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let num = args.remove(0).as_int() as usize;
	Ok(val.repeat(num).into())
}

pub fn replace(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let old = args.remove(0).as_string();
	let new = args.remove(0).as_string();
	Ok(val.replace(&old, &new).into())
}

pub fn reverse(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().rev().collect::<String>().into())
}

pub fn slice(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let beg = args.remove(0).as_int() as usize;
	let lim = args.remove(0).as_int() as usize;
	let val = val.chars().skip(beg).take(lim).collect::<String>();
	Ok(val.into())
}

pub fn slug(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(string::slug(&args.remove(0).as_string()).into())
}

pub fn split(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let chr = args.remove(0).as_string();
	let val = val.split(&chr).collect::<Vec<&str>>();
	Ok(val.into())
}

pub fn starts_with(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	let val = args.remove(0).as_string();
	let chr = args.remove(0).as_string();
	Ok(val.starts_with(&chr).into())
}

pub fn trim(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().trim().into())
}

pub fn uppercase(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().to_uppercase().into())
}

pub fn words(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().split(' ').collect::<Vec<&str>>().into())
}
