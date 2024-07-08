use js::{
	class::{ClassId, JsClass, OwnedBorrow, Readable, Trace},
	prelude::{Coerced, Opt},
	Ctx, Exception, FromJs, Result, Value,
};
use reblessive::tree::Stk;

use crate::{
	ctx::Context,
	dbs::{Attach, Options},
	doc::CursorDoc,
	sql::Value as SurValue,
};

#[allow(clippy::module_inception)]
mod classes;

pub use classes::Query;

pub const QUERY_DATA_PROP_NAME: &str = "__query_context__";

/// A class to carry the data to run subqueries.
#[non_exhaustive]
pub struct QueryContext<'js> {
	pub context: &'js Context<'js>,
	pub opt: &'js Options,
	pub doc: Option<&'js CursorDoc<'js>>,
}

impl<'js> Trace<'js> for QueryContext<'js> {
	fn trace<'a>(&self, _tracer: js::class::Tracer<'a, 'js>) {}
}

impl<'js> JsClass<'js> for QueryContext<'js> {
	const NAME: &'static str = "QueryContext";

	type Mutable = Readable;

	fn class_id() -> &'static js::class::ClassId {
		static ID: ClassId = ClassId::new();
		&ID
	}

	fn prototype(_ctx: &js::Ctx<'js>) -> Result<Option<js::Object<'js>>> {
		Ok(None)
	}

	fn constructor(_ctx: &js::Ctx<'js>) -> Result<Option<js::function::Constructor<'js>>> {
		Ok(None)
	}
}

/// The function which runs the query.
#[js::function]
pub async fn query<'js>(
	ctx: Ctx<'js>,
	query: Value<'js>,
	variables: Opt<classes::QueryVariables>,
) -> Result<SurValue> {
	let this = ctx.globals().get::<_, OwnedBorrow<'js, QueryContext<'js>>>(QUERY_DATA_PROP_NAME)?;

	let mut borrow_store = None;
	let mut query_store = None;

	let query = if query.is_object() {
		let borrow = OwnedBorrow::<Query>::from_js(&ctx, query)?;
		&**borrow_store.insert(borrow)
	} else {
		let Coerced(query_text) = Coerced::<String>::from_js(&ctx, query)?;
		query_store.insert(classes::Query::new(ctx.clone(), query_text, variables)?)
	};

	let context = Context::new(this.context);
	let context = query
		.clone()
		.vars
		.attach(context)
		.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))?;

	let value = Stk::enter_run(|stk| query.query.compute(stk, &context, this.opt, this.doc))
		.await
		.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))?;
	Result::Ok(value)
}
