use std::cell::RefCell;

use js::class::{JsClass, OwnedBorrow, Readable, Trace};
use js::prelude::{Coerced, Opt};
use js::{Ctx, Exception, FromJs, JsLifetime, Promise, Result, Value};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;

mod classes;

pub(crate) use classes::Query;

use crate::ctx::Context;

/// A class to carry the data to run subqueries.
#[derive(JsLifetime)]
pub struct QueryContext<'js> {
	pub context: &'js FrozenContext,
	pub opt: &'js Options,
	pub doc: Option<&'js CursorDoc>,
	pub pending: RefCell<Option<Promise<'js>>>,
}

impl<'js> Trace<'js> for QueryContext<'js> {
	fn trace<'a>(&self, _tracer: js::class::Tracer<'a, 'js>) {}
}

impl<'js> JsClass<'js> for QueryContext<'js> {
	const NAME: &'static str = "QueryContext";

	type Mutable = Readable;

	fn prototype(_ctx: &js::Ctx<'js>) -> Result<Option<js::Object<'js>>> {
		Ok(None)
	}

	fn constructor(_ctx: &js::Ctx<'js>) -> Result<Option<js::function::Constructor<'js>>> {
		Ok(None)
	}
}

#[js::function]
pub fn query<'js>(
	ctx: Ctx<'js>,
	query: Value<'js>,
	variables: Opt<classes::QueryVariables>,
) -> Result<Promise<'js>> {
	let ctx_clone = ctx.clone();
	let query_ctx = ctx_clone.userdata::<QueryContext<'js>>().expect("query context should be set");

	let mut pending_borrow = query_ctx.pending.borrow_mut();
	let pending_query_future = pending_borrow.as_ref().map(|x| x.clone().into_future::<()>());

	let promise = Promise::wrap_future(&ctx_clone, async move {
		let query_ctx = ctx.userdata::<QueryContext<'js>>().expect("query context should be set");

		// Wait for the previous query on this context to finish, so that at most one
		// query runs at a time.
		if let Some(x) = pending_query_future {
			let _ = x.await;
		}

		let mut borrow_store = None;
		let mut query_store = None;

		let res = async {
			let query = if query.is_object() {
				let borrow = OwnedBorrow::<Query>::from_js(&ctx, query)?;
				&**borrow_store.insert(borrow)
			} else {
				let Coerced(query_text) = Coerced::<String>::from_js(&ctx, query)?;
				query_store.insert(Query::new(ctx.clone(), query_text, variables)?)
			};

			// The subquery context is a child of the caller's context, so every clock
			// read inside the subquery uses the caller statement's timestamp. Neither
			// this function nor the entry points it calls install a clock of their own.
			let mut context = Context::new_child(query_ctx.context);
			if let Some(v) = query.clone().vars {
				context
					.attach_variables(v)
					.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))?;
			};
			let context = context.freeze();

			// The text is compiled as a nested query of its own, never into
			// the program running the script.
			let value = crate::exec::compiles::nested_query(async {
				let exec = crate::exec::entry::frame(&context, query_ctx.opt, query_ctx.doc).await;
				crate::exec::entry::scalar(&exec, &query.query).await
			})
			.await;
			value.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))
		}
		.await;

		*query_ctx.pending.borrow_mut() = None;

		res
	})?;

	*pending_borrow = Some(promise.clone());

	Ok(promise)
}
