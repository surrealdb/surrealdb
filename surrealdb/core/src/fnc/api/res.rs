use std::collections::BTreeMap;

use anyhow::{Result, bail};
use http::header::CONTENT_TYPE;
use http::{HeaderName, HeaderValue, StatusCode};
use reblessive::tree::Stk;
use surrealdb_types::SurrealValue;

use crate::api::middleware::common::BodyStrategy;
use crate::api::middleware::res::output_body_strategy;
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::{FromPublic, Optional};
use crate::rpc::format;
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::PublicBytes;
use crate::val::{Bytes, Closure, Object, Value};

pub async fn body(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req.clone().into()]).await?;
	let mut res: ApiResponse = res.try_into()?;

	let strategy = strategy.map(|x| x.0).unwrap_or_default();
	let Some(strategy) = output_body_strategy(&req.headers, strategy) else {
		bail!("No output strategy was possible for this API request");
	};

	match strategy {
		BodyStrategy::Auto | BodyStrategy::Json => {
			res.body = PublicBytes::from(format::json::encode(res.body)?).into_value();
			res.headers.insert(CONTENT_TYPE, "application/json".try_into()?);
		}
		BodyStrategy::Cbor => {
			res.body = PublicBytes::from(format::cbor::encode(res.body)?).into_value();
			res.headers.insert(CONTENT_TYPE, "application/cbor".try_into()?);
		}
		BodyStrategy::Flatbuffers => {
			res.body = PublicBytes::from(format::flatbuffers::encode(&res.body)?).into_value();
			res.headers.insert(CONTENT_TYPE, "application/vnd.surrealdb.flatbuffers".try_into()?);
		}
		BodyStrategy::Bytes => {
			res.body =
				PublicBytes::from(convert_public_value_to_internal(res.body).cast_to::<Bytes>()?.0)
					.into_value();
			res.headers.insert(CONTENT_TYPE, "application/octet-stream".try_into()?);
		}
		BodyStrategy::Plain => {
			let text = convert_public_value_to_internal(res.body).cast_to::<String>()?;
			res.body = PublicBytes::from(text.into_bytes()).into_value();
			res.headers.insert(CONTENT_TYPE, "text/plain".try_into()?);
		}
		BodyStrategy::Native => {
			res.headers.insert(CONTENT_TYPE, "application/vnd.surrealdb.native".try_into()?);
		}
	}

	Ok(res.into())
}

pub async fn status(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, status): (Value, Box<Closure>, i64),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req]).await?;
	let mut res: ApiResponse = res.try_into()?;

	let Ok(status) = u16::try_from(status) else {
		bail!("Invalid status code")
	};

	let Ok(status) = StatusCode::try_from(status) else {
		bail!("Invalid status code")
	};

	res.status = status;
	Ok(res.into())
}

pub async fn header(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, name, Optional(value)): (Value, Box<Closure>, String, Optional<String>),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req]).await?;
	let mut res: ApiResponse = res.try_into()?;

	let name: HeaderName = name.parse()?;
	let old = if let Some(value) = value {
		let value: HeaderValue = value.parse()?;
		res.headers.insert(name, value)
	} else {
		res.headers.remove(name)
	};

	if let Some(old) = old {
		Ok(Value::from(old.to_str()?))
	} else {
		Ok(Value::None)
	}
}

pub async fn headers(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, headers): (Value, Box<Closure>, BTreeMap<String, Option<String>>),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req]).await?;
	let mut res: ApiResponse = res.try_into()?;
	let mut old_headers = Object::default();

	for (k, value) in headers {
		let name: HeaderName = k.parse()?;
		let old = if let Some(value) = value {
			let value: HeaderValue = value.parse()?;
			res.headers.insert(name, value)
		} else {
			res.headers.remove(name)
		};

		if let Some(old) = old {
			old_headers.insert(k, Value::from(old.to_str()?));
		}
	}

	Ok(Value::Object(old_headers))
}
