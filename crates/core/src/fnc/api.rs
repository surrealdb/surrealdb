use std::collections::BTreeMap;

use anyhow::Result;
use http::HeaderMap;
use reblessive::tree::Stk;

use super::args::Optional;
use crate::api::body::ApiBody;
use crate::api::invocation::ApiInvocation;
use crate::catalog::{ApiDefinition, ApiMethod};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::val::{Object, Value};

pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &Context, &Options),
	(path, Optional(opts)): (String, Optional<Object>),
) -> Result<Value> {
	let (body, method, query, headers) = if let Some(opts) = opts {
		let body = match opts.get("body") {
			Some(v) => v.to_owned(),
			_ => Default::default(),
		};

		let method = if let Some(v) = opts.get("method") {
			ApiMethod::try_from(v)?
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

	if let Some((api, params)) = ApiDefinition::find_definition(&apis, segments, method) {
		let invocation = ApiInvocation {
			params,
			method,
			query,
			headers,
		};

		match invocation.invoke_with_context(stk, ctx, opt, api, ApiBody::from_value(body)).await {
			Ok(Some(v)) => Ok(v.0.into_response_value()?),
			Err(e) => Err(e),
			_ => Ok(Value::None),
		}
	} else {
		Ok(Value::None)
	}
}
