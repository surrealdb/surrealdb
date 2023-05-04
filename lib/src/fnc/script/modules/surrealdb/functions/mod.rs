use crate::ctx::Context;
use crate::fnc;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::Value;
use js::{Async, Result};

mod array;
mod crypto;
mod duration;
mod geo;
mod http;
mod is;
mod math;
mod meta;
mod parse;
mod rand;
mod session;
mod string;
mod time;
mod r#type;

pub struct Package;

impl_module_def!(
	Package,
	"", // root path
	"array" => (array::Package),
	"count" => run,
	"crypto" => (crypto::Package),
	"duration" => (duration::Package),
	"geo" => (geo::Package),
	"http" => (http::Package),
	"is" => (is::Package),
	"math" => (math::Package),
	"meta" => (meta::Package),
	"not" => run,
	"parse" => (parse::Package),
	"rand" => (rand::Package),
	"array" => (array::Package),
	"session" => (session::Package),
	"sleep" => fut Async,
	"string" => (string::Package),
	"time" => (time::Package),
	"type" => (r#type::Package)
);

fn run(name: &str, args: Vec<Value>) -> Result<Value> {
	// Create a default context
	let ctx = Context::background();
	// Process the called function
	let res = fnc::synchronous(&ctx, name, args);
	// Convert any response error
	res.map_err(|err| js::Error::Exception {
		message: err.to_string(),
		file: String::from(""),
		line: -1,
		stack: String::from(""),
	})
}

async fn fut(name: &str, args: Vec<Value>) -> Result<Value> {
	// Create a default context
	let ctx = Context::background();
	// Process the called function
	let res = fnc::asynchronous(&ctx, name, args).await;
	// Convert any response error
	res.map_err(|err| js::Error::Exception {
		message: err.to_string(),
		file: String::from(""),
		line: -1,
		stack: String::from(""),
	})
}
