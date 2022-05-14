use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

pub async fn head(_ctx: &Context<'_>, args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 | 2 => todo!(),
		_ => Err(Error::InvalidArguments {
			name: String::from("http::head"),
			message: String::from("The function expects 1 or 2 arguments."),
		}),
	}
}

pub async fn get(_ctx: &Context<'_>, args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 | 2 => todo!(),
		_ => Err(Error::InvalidArguments {
			name: String::from("http::get"),
			message: String::from("The function expects 1 or 2 arguments."),
		}),
	}
}

pub async fn put(_ctx: &Context<'_>, args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 | 2 | 3 => todo!(),
		_ => Err(Error::InvalidArguments {
			name: String::from("http::put"),
			message: String::from("The function expects 1, 2, or 3 arguments."),
		}),
	}
}

pub async fn post(_ctx: &Context<'_>, args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 | 2 | 3 => todo!(),
		_ => Err(Error::InvalidArguments {
			name: String::from("http::post"),
			message: String::from("The function expects 1, 2, or 3 arguments."),
		}),
	}
}

pub async fn patch(_ctx: &Context<'_>, args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 | 2 | 3 => todo!(),
		_ => Err(Error::InvalidArguments {
			name: String::from("http::patch"),
			message: String::from("The function expects 1, 2, or 3 arguments."),
		}),
	}
}

pub async fn delete(_ctx: &Context<'_>, args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 | 2 => todo!(),
		_ => Err(Error::InvalidArguments {
			name: String::from("http::delete"),
			message: String::from("The function expects 1 or 2 arguments."),
		}),
	}
}
