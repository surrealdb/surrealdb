use http::HeaderMap;
use reblessive::tree::Stk;
use std::collections::BTreeMap;

use crate::{
	api::{body::ApiBody, invocation::ApiInvocation, method::Method},
	ctx::Context,
	dbs::{capabilities::ExperimentalTarget, Options},
	err::Error,
	sql::{statements::FindApi, Object, Value},
};

pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &Context, &Options),
	(path, opts): (String, Option<Object>),
) -> Result<Value, Error> {
	if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::DefineApi) {
		return Err(Error::InvalidFunction {
			name: "api::invoke".to_string(),
			message: "Experimental feature is disabled".to_string(),
		});
	}

	let (body, method, query, headers) = if let Some(opts) = opts {
		let body = match opts.get("body") {
			Some(v) => v.to_owned(),
			_ => Default::default(),
		};

		let method = if let Some(v) = opts.get("method") {
			Method::try_from(v)?
		} else {
			Method::Get
		};

		let query: BTreeMap<String, String> = if let Some(v) = opts.get("query") {
			v.to_owned().convert_to_object()?.try_into()?
		} else {
			Default::default()
		};

		let headers: HeaderMap = if let Some(v) = opts.get("headers") {
			v.to_owned().convert_to_object()?.try_into()?
		} else {
			Default::default()
		};

		(body, method, query, headers)
	} else {
		(Default::default(), Method::Get, Default::default(), Default::default())
	};

	let ns = opt.ns()?;
	let db = opt.db()?;
	let apis = ctx.tx().all_db_apis(ns, db).await?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	if let Some((api, params)) = apis.as_ref().find_api(segments, method) {
		let invocation = ApiInvocation {
			params,
			method,
			query,
			headers,
		};

		match invocation.invoke_with_context(stk, ctx, opt, api, ApiBody::from_value(body)).await {
			Ok(Some(v)) => v.0.try_into(),
			Err(e) => Err(e),
			_ => Ok(Value::None),
		}
	} else {
		Ok(Value::None)
	}
}
