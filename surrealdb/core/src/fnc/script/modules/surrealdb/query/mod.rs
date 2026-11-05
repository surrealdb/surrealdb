use std::cell::{Cell, RefCell};

use js::class::{JsClass, OwnedBorrow, Readable, Trace};
use js::prelude::{Coerced, Opt};
use js::{Ctx, Exception, FromJs, JsLifetime, Promise, Result, Value};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::catch_scope::CatchScope;

mod classes;

pub(crate) use classes::Query;

use crate::ctx::Context;

/// A class to carry the data to run subqueries.
#[derive(JsLifetime)]
pub struct QueryContext<'js> {
	pub context: &'js FrozenContext,
	pub opt: &'js Options,
	pub doc: Option<&'js CursorDoc>,
	/// The last `surrealdb.query` the script started, which the next one waits
	/// for, so that the script's queries run one at a time in the order it
	/// started them.
	pub pending: RefCell<Option<Promise<'js>>>,
	/// How many `surrealdb.query` calls the script has started. A query clears
	/// [`Self::pending`] when it finishes only while it is still the last one
	/// started, so a query started after it keeps its place behind the one
	/// before it.
	pub started: Cell<u64>,
	/// The place of each call the script makes on its transaction ([`caught`]):
	/// a call that needs a savepoint holds it exclusively until the savepoint
	/// is closed, and every other call shares it, so no call runs while
	/// another's savepoint is open.
	pub writing: tokio::sync::RwLock<()>,
}

/// Runs `unit`, a call the script may catch the rejection of, so that a call
/// that fails leaves none of its writes in the transaction: the script goes on
/// in that transaction, which commits whatever the script made of the
/// rejection ([`crate::exec::catch_scope`]).
///
/// A call that needs a savepoint ([`CatchScope::needed`]: it `may_write`, or
/// the transaction defined a sequence it may draw from) runs behind one of its
/// own, holding [`QueryContext::writing`] exclusively until the savepoint is
/// closed: savepoints are a stack that two calls the script awaits together
/// would otherwise interleave, and a call running beside an open savepoint
/// would read writes its rollback takes back, or have its own sequence draws
/// taken back with them. Any other call shares the place with the others like
/// it.
pub(crate) async fn caught<T>(
	query_ctx: &QueryContext<'_>,
	may_write: bool,
	unit: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
	let txn = query_ctx.context.try_tx();
	if !CatchScope::needed(txn, may_write) {
		let reading = query_ctx.writing.read().await;
		// A call holding the exclusive place may have defined a sequence while
		// this one waited for its share.
		if !CatchScope::needed(txn, may_write) {
			let answered = unit.await;
			drop(reading);
			return answered;
		}
	}
	let _writing = query_ctx.writing.write().await;
	let scope = CatchScope::open(txn, may_write).await?;
	let answered = unit.await;
	scope.end(answered.is_err()).await?;
	answered
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
	let started = query_ctx.started.get() + 1;
	query_ctx.started.set(started);

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
			// Running arbitrary SurrealQL through the bridge is an arbitrary
			// query, so it must pass the same subject gate the request front door
			// applies (mirrors fnc::eval).
			if !query_ctx.context.get_capabilities().allows_query(
				&crate::dbs::capabilities::ArbitraryQueryTarget::from(query_ctx.opt.auth.as_ref()),
			) {
				return Err(Exception::throw_message(
					&ctx,
					&crate::dbs::capabilities::Error::FunctionNotAllowed(
						"surrealdb.query".to_string(),
					)
					.to_string(),
				));
			}
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
			let value = caught(
				&query_ctx,
				!query.query.read_only(),
				crate::exec::compiles::nested_query(async {
					let exec =
						crate::exec::entry::frame(&context, query_ctx.opt, query_ctx.doc).await;
					crate::exec::entry::scalar(&exec, &query.query).await
				}),
			)
			.await;
			value.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))
		}
		.await;

		if query_ctx.started.get() == started {
			*query_ctx.pending.borrow_mut() = None;
		}

		res
	})?;

	*pending_borrow = Some(promise.clone());

	Ok(promise)
}
