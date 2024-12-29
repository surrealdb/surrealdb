use std::sync::Arc;

use reblessive::tree::Stk;

use crate::{api::method::Method, ctx::Context, dbs::Options, err::Error, iam::{Auth, Role}, sql::{statements::FindApi, Object, Value}, ApiInvocation};

pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &Context, &Options),
	(path, opts,): (String, Option<Object>),
) -> Result<Value, Error> {
	let (body, method, query) = if let Some(opts) = opts {
		let body = match opts.get("body") {
			Some(v) => v.to_owned(),
			_ => Value::None,
		};

		let method = if let Some(v) = opts.get("method") {
			Method::try_from(v)?
		} else {
			Method::Get
		};

		let query = if let Some(v) = opts.get("query") {
			v.to_owned().convert_to_object()?
		} else {
			Object::default()
		};

		(body, method, query)
	} else {
		(Value::None, Method::Get, Object::default())
	};

    let ns = opt.ns()?;
    let db = opt.db()?;
	let tx = ctx.tx();
	let apis = tx.all_db_apis(&ns, &db).await?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();
	
	if let Some((api, params)) = apis.as_ref().find_api(segments) {
		let values = vec![
			("access", ctx.value("access").map(|v| v.to_owned()).unwrap_or_default()),
			("auth", ctx.value("auth").map(|v| v.to_owned()).unwrap_or_default()),
			("token", ctx.value("token").map(|v| v.to_owned()).unwrap_or_default()),
			("session", ctx.value("session").map(|v| v.to_owned()).unwrap_or_default()),
		];

		let auth = Arc::new(Auth::for_db(Role::Owner, ns, db));
		let opt = &opt.clone().with_auth(auth);
		let invocation = ApiInvocation {
			params,
			body,
			method,
			query,
			session: None,
			values,
		};

		match api.invoke_with_context(stk, ctx, opt, invocation).await {
			Ok(Some(v)) => Ok(v),
			Err(e) => return Err(e),
			_ => Ok(Value::None),
		}
	} else {
		Ok(Value::None)
	}
}