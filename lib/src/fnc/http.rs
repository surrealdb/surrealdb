use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::util::http;
use crate::sql::object::Object;
use crate::sql::value::Value;

pub async fn head(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Strand(uri) => match args.remove(0) {
				Value::Object(opt) => http::head(uri, opt).await,
				_ => Err(Error::InvalidArguments {
					name: String::from("http::head"),
					message: String::from("The second argument should be an object."),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("http::head"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => http::head(uri, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::head"),
				message: String::from("The first argument should be a string."),
			}),
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("http::head"),
			message: String::from("The function expects 1 or 2 arguments."),
		}),
	}
}

pub async fn get(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Strand(uri) => match args.remove(0) {
				Value::Object(opt) => http::get(uri, opt).await,
				_ => Err(Error::InvalidArguments {
					name: String::from("http::get"),
					message: String::from("The second argument should be an object."),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("http::get"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => http::get(uri, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::get"),
				message: String::from("The first argument should be a string."),
			}),
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("http::get"),
			message: String::from("The function expects 1 or 2 arguments."),
		}),
	}
}

pub async fn put(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		3 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => match args.remove(0) {
				Value::Object(opts) => http::put(uri, val, opts).await,
				_ => Err(Error::InvalidArguments {
					name: String::from("http::put"),
					message: String::from("The third argument should be an object."),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("http::put"),
				message: String::from("The first argument should be a string."),
			}),
		},
		2 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => http::put(uri, val, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::put"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => http::put(uri, Value::Null, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::put"),
				message: String::from("The first argument should be a string."),
			}),
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("http::put"),
			message: String::from("The function expects 1, 2, or 3 arguments."),
		}),
	}
}

pub async fn post(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		3 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => match args.remove(0) {
				Value::Object(opts) => http::post(uri, val, opts).await,
				_ => Err(Error::InvalidArguments {
					name: String::from("http::post"),
					message: String::from("The third argument should be an object."),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("http::post"),
				message: String::from("The first argument should be a string."),
			}),
		},
		2 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => http::post(uri, val, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::post"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => http::post(uri, Value::Null, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::post"),
				message: String::from("The first argument should be a string."),
			}),
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("http::post"),
			message: String::from("The function expects 1, 2, or 3 arguments."),
		}),
	}
}

pub async fn patch(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		3 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => match args.remove(0) {
				Value::Object(opts) => http::patch(uri, val, opts).await,
				_ => Err(Error::InvalidArguments {
					name: String::from("http::patch"),
					message: String::from("The third argument should be an object."),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("http::patch"),
				message: String::from("The first argument should be a string."),
			}),
		},
		2 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => http::patch(uri, val, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::patch"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => http::patch(uri, Value::Null, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::patch"),
				message: String::from("The first argument should be a string."),
			}),
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("http::patch"),
			message: String::from("The function expects 1, 2, or 3 arguments."),
		}),
	}
}

pub async fn delete(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Strand(uri) => match args.remove(0) {
				Value::Object(opt) => http::delete(uri, opt).await,
				_ => Err(Error::InvalidArguments {
					name: String::from("http::delete"),
					message: String::from("The second argument should be an object."),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("http::delete"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => http::delete(uri, Object::default()).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::delete"),
				message: String::from("The first argument should be a string."),
			}),
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("http::delete"),
			message: String::from("The function expects 1 or 2 arguments."),
		}),
	}
}
