use std::collections::BTreeMap;

use anyhow::{Result, bail};
use http::{HeaderName, HeaderValue, StatusCode};
use http::header::CONTENT_TYPE;
use crate::fnc::args::{FromPublic, Optional};
use crate::rpc::format;

use crate::types::PublicBytes;
use crate::val::{Bytes, Object};
use crate::{api::{middleware::api_x::{common::BodyStrategy, res::output_body_strategy}, request::ApiRequest, response::ApiResponse}, ctx::FrozenContext, sql::expression::convert_public_value_to_internal, val::{Value, convert_value_to_public_value}};
use surrealdb_types::SurrealValue;

pub async fn body(
    ctx: &FrozenContext,
    (FromPublic(mut res), Optional(strategy)): (FromPublic<ApiResponse>, Optional<FromPublic<BodyStrategy>>),
) -> Result<Value> {
    let req: ApiRequest = if let Some(v) = ctx.value("request") {
        convert_value_to_public_value(v.clone())?.into_t()?
    } else {
        bail!("No $request parameter present");
    };

    let strategy = strategy.map(|x| x.0).unwrap_or_default();
    let Some(strategy) = output_body_strategy(&req.headers, strategy) else {
        bail!("No output strategy was possible for this API request");
    };

    match strategy {
        BodyStrategy::Auto | BodyStrategy::Json => {
            res.body = PublicBytes::from(format::json::encode(res.body)?).into_value();
            res.headers.insert(CONTENT_TYPE, "application/json".try_into()?);
        },
        BodyStrategy::Cbor => {
            res.body = PublicBytes::from(format::cbor::encode(res.body)?).into_value();
            res.headers.insert(CONTENT_TYPE, "application/cbor".try_into()?);
        }
        BodyStrategy::Flatbuffers => {
            res.body = PublicBytes::from(format::flatbuffers::encode(&res.body)?).into_value();
            res.headers.insert(CONTENT_TYPE, "application/vnd.surrealdb.flatbuffers".try_into()?);
        }
        BodyStrategy::Bytes => {
            res.body = PublicBytes::from(convert_public_value_to_internal(res.body).cast_to::<Bytes>()?.0).into_value();
            res.headers.insert(CONTENT_TYPE, "application/octet-stream".try_into()?);
        }
        BodyStrategy::Plain => {
            let text = convert_public_value_to_internal(res.body).cast_to::<String>()?;
            res.body = PublicBytes::from(text.into_bytes()).into_value();
            res.headers.insert(CONTENT_TYPE, "text/plain".try_into()?);
        }
    }

    Ok(convert_public_value_to_internal(res.into_value()))
}

pub fn status((FromPublic(mut res), status): (FromPublic<ApiResponse>, i64)) -> Result<Value> {
    let Ok(status) = u16::try_from(status) else {
        bail!("Invalid status code")
    };

    let Ok(status) = StatusCode::try_from(status) else {
        bail!("Invalid status code")
    };

    res.status = status;
    Ok(convert_public_value_to_internal(res.into_value()))
}

pub fn header((FromPublic(mut res), name, Optional(value)): (FromPublic<ApiResponse>, String, Optional<String>)) -> Result<Value> {
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

pub fn headers((FromPublic(mut res), headers): (FromPublic<ApiResponse>, BTreeMap<String, Option<String>>)) -> Result<Value> {
    let mut old_headers = Object::default();

    for (k, value) in headers.into_iter() {
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