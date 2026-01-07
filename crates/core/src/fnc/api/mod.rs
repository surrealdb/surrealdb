use std::collections::BTreeMap;

use anyhow::Result;
use http::HeaderMap;
use reblessive::tree::Stk;
use surrealdb_types::SurrealValue;

use super::args::Optional;
use crate::api::body::ApiBody;
use crate::api::invocation::ApiInvocation;
use crate::api::request::ApiRequest;
use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiDefinition, ApiMethod};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::fnc::args::FromPublic;
use crate::sql::expression::convert_public_value_to_internal;
use crate::val::{Duration, Object, Value};

pub mod req;
pub mod res;

pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &FrozenContext, &Options),
	(path, Optional(opts)): (String, Optional<Object>),
) -> Result<Value> {
	let (body, method, query, headers) = if let Some(opts) = opts {
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
			Ok(Some(v)) => Ok(convert_public_value_to_internal(v.0.into_value())),
			Err(e) => Err(e),
			_ => Ok(Value::None),
		}
	} else {
		Ok(Value::None)
	}
}

pub fn timeout((FromPublic(mut req), Optional(timeout)): (FromPublic<ApiRequest>, Optional<Duration>)) -> Result<Value> {
    req.timeout = timeout.map(Into::into);
    Ok(convert_public_value_to_internal(req.into_value()))
}
