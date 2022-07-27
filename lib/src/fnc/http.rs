use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

#[cfg(not(feature = "http"))]
pub async fn head(_: &Context<'_>, _: Vec<Value>) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn get(_: &Context<'_>, _: Vec<Value>) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn put(_: &Context<'_>, _: Vec<Value>) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn post(_: &Context<'_>, _: Vec<Value>) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn patch(_: &Context<'_>, _: Vec<Value>) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn delete(_: &Context<'_>, _: Vec<Value>) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(feature = "http")]
pub async fn head(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Strand(uri) => match args.remove(0) {
				Value::Object(opt) => crate::fnc::util::http::head(uri, opt).await,
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
			Value::Strand(uri) => crate::fnc::util::http::head(uri, None).await,
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

#[cfg(feature = "http")]
pub async fn get(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Strand(uri) => match args.remove(0) {
				Value::Object(opt) => crate::fnc::util::http::get(uri, opt).await,
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
			Value::Strand(uri) => crate::fnc::util::http::get(uri, None).await,
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

#[cfg(feature = "http")]
pub async fn put(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		3 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => match args.remove(0) {
				Value::Object(opts) => crate::fnc::util::http::put(uri, val, opts).await,
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
			(Value::Strand(uri), val) => crate::fnc::util::http::put(uri, val, None).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::put"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => crate::fnc::util::http::put(uri, Value::Null, None).await,
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

#[cfg(feature = "http")]
pub async fn post(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		3 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => match args.remove(0) {
				Value::Object(opts) => crate::fnc::util::http::post(uri, val, opts).await,
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
			(Value::Strand(uri), val) => crate::fnc::util::http::post(uri, val, None).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::post"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => crate::fnc::util::http::post(uri, Value::Null, None).await,
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

#[cfg(feature = "http")]
pub async fn patch(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		3 => match (args.remove(0), args.remove(0)) {
			(Value::Strand(uri), val) => match args.remove(0) {
				Value::Object(opts) => crate::fnc::util::http::patch(uri, val, opts).await,
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
			(Value::Strand(uri), val) => crate::fnc::util::http::patch(uri, val, None).await,
			_ => Err(Error::InvalidArguments {
				name: String::from("http::patch"),
				message: String::from("The first argument should be a string."),
			}),
		},
		1 => match args.remove(0) {
			Value::Strand(uri) => crate::fnc::util::http::patch(uri, Value::Null, None).await,
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

#[cfg(feature = "http")]
pub async fn delete(_: &Context<'_>, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Strand(uri) => match args.remove(0) {
				Value::Object(opt) => crate::fnc::util::http::delete(uri, opt).await,
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
			Value::Strand(uri) => crate::fnc::util::http::delete(uri, None).await,
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
