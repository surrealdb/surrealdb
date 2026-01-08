use std::collections::BTreeMap;

use anyhow::Result;
use http::header::{ACCEPT, CONTENT_TYPE};
use http::HeaderMap;
use reblessive::tree::Stk;
use surrealdb_types::SurrealValue;

use super::args::Optional;
use crate::api::body::ApiBody;
use crate::api::invocation::ApiInvocation;
use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiDefinition, ApiMethod};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::{Closure, Duration, Object, Value};

pub mod req;
pub mod res;

pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &FrozenContext, &Options),
	(path, Optional(opts)): (String, Optional<Object>),
) -> Result<Value> {
	let (body, method, query, mut headers) = if let Some(opts) = opts {
		let body = match opts.get("body") {
			Some(v) => v.to_owned(),
			_ => Default::default(),
		};

		let method = if let Some(v) = opts.get("method") {
			let public_val = crate::val::convert_value_to_public_value(v.clone())?;
			ApiMethod::from_value(public_val)?
		} else {
			ApiMethod::Get
		};

		let query: BTreeMap<String, String> = if let Some(v) = opts.get("query") {
			v.to_owned().cast_to::<Object>()?.try_into()?
		} else {
			Default::default()
		};

		let headers: HeaderMap = if let Some(v) = opts.get("headers") {
			v.to_owned().cast_to::<Object>()?.try_into()?
		} else {
			Default::default()
		};

		(body, method, query, headers)
	} else {
		(Default::default(), ApiMethod::Get, Default::default(), Default::default())
	};

	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let apis = ctx.tx().all_db_apis(ns, db).await?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	if !headers.contains_key(CONTENT_TYPE) {
		headers.insert(CONTENT_TYPE, "application/vnd.surrealdb.native".try_into()?);
	}

	if !headers.contains_key(ACCEPT) {
		// We'll accept anything, but we prefer native.
		headers.insert(ACCEPT, "application/vnd.surrealdb.native;q=0.9, */*;q=0.8".try_into()?);
	}

	if let Some((api, params)) = ApiDefinition::find_definition(&apis, segments, method) {
		let invocation = ApiInvocation {
			params: params.try_into()?,
			method,
			query,
			headers,
		};

		// Convert body to public value for ApiBody
		let public_body = crate::val::convert_value_to_public_value(body)?;
		match invocation
			.invoke_with_context(stk, ctx, opt, api, ApiBody::from_value(public_body))
			.await
		{
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
