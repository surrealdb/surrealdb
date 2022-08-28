use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::util::string;
use crate::sql::value::Value;

pub fn concat(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.into_iter().map(|x| x.as_string()).collect::<Vec<_>>().concat().into())
}

pub fn ends_with(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let args: [Value; 2] = args.try_into().unwrap();
	let [val, chr] = args.map(Value::as_string);
	Ok(val.ends_with(&chr).into())
}

pub fn join(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let mut args = args.into_iter().map(Value::as_string);
	let chr = args.next().unwrap();
	// FIXME: Use intersperse to avoid intermediate allocation once stable
	// https://github.com/rust-lang/rust/issues/79524
	let val = args.collect::<Vec<_>>().join(&chr);
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

pub fn repeat(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let [val_arg, num_arg]: [Value; 2] = args.try_into().unwrap();
	let val = val_arg.as_string();
	let num = num_arg.as_int() as usize;
	Ok(val.repeat(num).into())
}

pub fn replace(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let args: [Value; 3] = args.try_into().unwrap();
	let [val, old, new] = args.map(Value::as_string);
	Ok(val.replace(&old, &new).into())
}

pub fn reverse(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().rev().collect::<String>().into())
}

pub fn slice(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let [val_arg, beg_arg, lim_arg]: [Value; 3] = args.try_into().unwrap();
	let val = val_arg.as_string();
	let beg = beg_arg.as_int() as usize;
	let lim = lim_arg.as_int() as usize;
	let val = val.chars().skip(beg).take(lim).collect::<String>();
	Ok(val.into())
}

pub fn slug(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(string::slug(&args.remove(0).as_string()).into())
}

pub fn split(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let args: [Value; 2] = args.try_into().unwrap();
	let [val, chr] = args.map(Value::as_string);
	let val = val.split(&chr).collect::<Vec<&str>>();
	Ok(val.into())
}

pub fn starts_with(_: &Context, args: Vec<Value>) -> Result<Value, Error> {
	let args: [Value; 2] = args.try_into().unwrap();
	let [val, chr] = args.map(Value::as_string);
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
