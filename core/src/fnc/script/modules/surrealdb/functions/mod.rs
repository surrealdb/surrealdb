use crate::fnc;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::Value;
use js::class::OwnedBorrow;
use js::prelude::Async;
use js::Result;
use reblessive::tree::Stk;

use super::query::{QueryContext, QUERY_DATA_PROP_NAME};

mod array;
mod bytes;
mod crypto;
mod duration;
mod encoding;
mod geo;
mod http;
mod math;
mod object;
mod parse;
mod rand;
mod record;
mod search;
mod session;
mod string;
mod time;
mod r#type;
mod value;
mod vector;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"", // root path
	"array" => (array::Package),
	"bytes" => (bytes::Package),
	"count" => run,
	"crypto" => (crypto::Package),
	"duration" => (duration::Package),
	"encoding" => (encoding::Package),
	"geo" => (geo::Package),
	"http" => (http::Package),
	"math" => (math::Package),
	"not" => run,
	"object" => (object::Package),
	"parse" => (parse::Package),
	"rand" => (rand::Package),
	"record" => (record::Package),
	"search" => (search::Package),
	"session" => (session::Package),
	"sleep" => fut Async,
	"string" => (string::Package),
	"time" => (time::Package),
	"type" => (r#type::Package),
	"value" => (value::Package),
	"vector" => (vector::Package)
);

fn run(js_ctx: js::Ctx<'_>, name: &str, args: Vec<Value>) -> Result<Value> {
	let this = js_ctx.globals().get::<_, OwnedBorrow<QueryContext>>(QUERY_DATA_PROP_NAME)?;
	// Process the called function
	let res = fnc::synchronous(this.context, this.doc, name, args);
	// Convert any response error
	res.map_err(|err| {
		js::Exception::from_message(js_ctx, &err.to_string())
			.map(js::Exception::throw)
			.unwrap_or(js::Error::Exception)
	})
}

async fn fut(js_ctx: js::Ctx<'_>, name: &str, args: Vec<Value>) -> Result<Value> {
	let this = js_ctx.globals().get::<_, OwnedBorrow<QueryContext>>(QUERY_DATA_PROP_NAME)?;
	// Process the called function
	let res =
		Stk::enter_run(|stk| fnc::asynchronous(stk, this.context, this.opt, this.doc, name, args))
			.await;
	// Convert any response error
	res.map_err(|err| {
		js::Exception::from_message(js_ctx, &err.to_string())
			.map(js::Exception::throw)
			.unwrap_or(js::Error::Exception)
	})
}
