use anyhow::Result;
use http::header::{ACCEPT, CONTENT_TYPE};
use reblessive::tree::Stk;

use super::args::Optional;
use crate::api::invocation::process_api_request_with_stack;
use crate::api::request::ApiRequest;
use crate::catalog::providers::ApiProvider;
use crate::catalog::ApiDefinition;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::FromPublic;
use crate::val::{Closure, Duration, Value};

pub mod req;
pub mod res;

pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &FrozenContext, &Options),
	(path, Optional(req)): (String, Optional<FromPublic<ApiRequest>>),
) -> Result<Value> {
	let mut req = req.map(|x| x.0).unwrap_or_default();
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let apis = ctx.tx().all_db_apis(ns, db).await?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	if !req.headers.contains_key(CONTENT_TYPE) {
		req.headers.insert(CONTENT_TYPE, "application/vnd.surrealdb.native".try_into()?);
	}

	if !req.headers.contains_key(ACCEPT) {
		// We'll accept anything, but we prefer native.
		req.headers.insert(ACCEPT, "application/vnd.surrealdb.native;q=0.9, */*;q=0.8".try_into()?);
	}

	if let Some((api, params)) = ApiDefinition::find_definition(&apis, segments, req.method) {
		// TODO should find_definition just return PublicObject in the first place?
		req.params = params.try_into()?;
		match process_api_request_with_stack(stk, ctx, opt, api, req).await {
			Ok(Some(v)) => Ok(v.into()),
			Err(e) => Err(e),
			_ => Ok(Value::None),
		}
	} else {
		Ok(Value::None)
	}
}

// TODO can we actually just run the timeout here? 
// like we chain onto the next middleware and eventually 
// the matched route, so cant we timeout that invoke of the "next" function
pub async fn timeout(
    (stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, timeout): (Value, Box<Closure>, Duration)
) -> Result<Value> {
    let mut ctx = Context::new_isolated(ctx);
	ctx.add_timeout(*timeout)?;
	let ctx = &ctx.freeze();
    
	next.invoke(stk, ctx, opt, doc, vec![req]).await
}
