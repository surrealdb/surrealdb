use js::Result;
use js::prelude::Async;

use super::query::{QueryContext, caught};
use crate::ctx::FrozenContext;
use crate::exec::function::timezone_at;
use crate::expr::logical::EffectSet;
use crate::fnc;
use crate::fnc::script::modules::impl_module_def;
use crate::val::{Datetime, Value};

mod api;
mod array;
mod bytes;
mod crypto;
mod duration;
mod encoding;
mod enumeration;
mod eval;
mod file;
mod geo;
mod http;
mod interval;
mod math;
mod meta;
mod object;
mod parse;
mod rand;
mod record;
mod schema;
mod search;
mod sequence;
mod session;
mod set;
mod string;
mod time;
mod r#type;
mod value;
mod vector;

pub struct Package;

impl_module_def!(
	Package,
	"", // root path
	"api" => (api::Package),
	"array" => (array::Package),
	"bytes" => (bytes::Package),
	"count" => run,
	"crypto" => (crypto::Package),
	"duration" => (duration::Package),
	"encoding" => (encoding::Package),
	"enum" => (enumeration::Package),
	"eval" => (eval::Package),
	"file" => (file::Package),
	"geo" => (geo::Package),
	"http" => (http::Package),
	"interval" => (interval::Package),
	"math" => (math::Package),
	"meta" => (meta::Package),
	"not" => run,
	"object" => (object::Package),
	"parse" => (parse::Package),
	"rand" => (rand::Package),
	"record" => (record::Package),
	"search" => (search::Package),
	"sequence" => (sequence::Package),
	"session" => (session::Package),
	"set" => (set::Package),
	"sleep" => fut Async,
	"string" => (string::Package),
	"time" => (time::Package),
	"type" => (r#type::Package),
	"value" => (value::Package),
	"vector" => (vector::Package),
	"schema" => (schema::Package)
);

/// Whether the registry declares `name` a CLOCK read (`EffectSet::CLOCK`).
/// The set is the registry's, so a script and a statement cannot disagree on
/// which names read the clock.
fn reads_the_clock(ctx: &FrozenContext, name: &str) -> bool {
	ctx.function_registry().get(name).is_some_and(|f| f.effects().contains(EffectSet::CLOCK))
}

/// Runs a clock read from a script at the statement's clock instant. Every
/// clock read in one statement gets the same instant. If the context has no
/// statement clock, the wall clock is used.
///
/// `time::now` and `time::timezone` answer from that instant. For a function
/// that defaults an omitted datetime to the current time, the instant is
/// passed when the argument list is empty or its only argument is `NONE`.
/// Any other arguments are passed through unchanged, so arity and type errors
/// come from [`fnc::synchronous`].
fn run_clock_read(
	ctx: &FrozenContext,
	doc: Option<&crate::doc::CursorDoc>,
	name: &str,
	mut args: Vec<Value>,
) -> anyhow::Result<Value> {
	let clock = ctx.statement_clock().copied().unwrap_or_else(Datetime::now);
	match name {
		"time::now" if args.is_empty() => Ok(clock.into()),
		"time::timezone" if args.is_empty() => timezone_at(clock),
		"time::now" | "time::timezone" => fnc::synchronous(ctx, doc, name, args),
		_ => {
			if matches!(args.as_slice(), [] | [Value::None]) {
				args = vec![clock.into()];
			}
			fnc::synchronous(ctx, doc, name, args)
		}
	}
}

fn run(js_ctx: js::Ctx<'_>, name: &str, args: Vec<Value>) -> Result<Value> {
	let res = {
		let this = js_ctx.userdata::<QueryContext<'_>>().expect("query context should be set");
		// The bridge dispatches a built-in function, so it is bound by the
		// function capability (deny-funcs) like every other call site.
		if !this.context.get_capabilities().allows_function_name(name) {
			Err(crate::dbs::capabilities::Error::FunctionNotAllowed(name.to_string()).into())
		} else if reads_the_clock(this.context, name) {
			run_clock_read(this.context, this.doc, name, args)
		} else {
			fnc::synchronous(this.context, this.doc, name, args)
		}
	};
	// Convert any response error
	res.map_err(|err| {
		js::Exception::from_message(js_ctx, &err.to_string())
			.map(js::Exception::throw)
			.unwrap_or(js::Error::Exception)
	})
}

async fn fut(js_ctx: js::Ctx<'_>, name: &str, args: Vec<Value>) -> Result<Value> {
	let res = {
		let this = js_ctx.userdata::<QueryContext<'_>>().expect("query context should be set");
		// The bridge dispatches a built-in function, so it is bound by the
		// function capability (deny-funcs) like every other call site.
		if !this.context.get_capabilities().allows_function_name(name) {
			Err(crate::dbs::capabilities::Error::FunctionNotAllowed(name.to_string()).into())
		} else {
			// The active frame may refuse this builtin before it runs: a LIVE
			// clause refuses one that writes to an external system, and every
			// schema-clause frame one that advances state or blocks. The bridge
			// dispatches a builtin without passing the VM's own guard, so the
			// refusal is applied here, from the effects the registry declares and
			// the frame the options carry. Every refusable builtin (`http::post`,
			// `file::put`, `sequence::nextval`, `sleep`) is async, so the
			// synchronous bridge has none to refuse.
			let effects =
				this.context.function_registry().get(name).map_or(EffectSet::PURE, |f| f.effects());
			match crate::fnc::script::refuse_builtin_in_frame(this.opt.no_write, name, effects) {
				// A builtin that writes, such as `eval::surql`, is a call the
				// script may catch the rejection of.
				Ok(()) => {
					caught(
						&this,
						effects.contains(EffectSet::WRITES_DATA),
						fnc::asynchronous(this.context, this.opt, this.doc, name, args),
					)
					.await
				}
				Err(err) => Err(err),
			}
		}
	};
	// Convert any response error
	res.map_err(|err| {
		js::Exception::from_message(js_ctx, &err.to_string())
			.map(js::Exception::throw)
			.unwrap_or(js::Error::Exception)
	})
}
