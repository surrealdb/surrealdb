use std::cmp::Ordering::Equal;
use std::str::FromStr;

use anyhow::Result;
use http::header::{ACCEPT, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue};
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::{PublicBytes, PublicValue};
use crate::val::Bytes;
use mime::{APPLICATION_JSON, APPLICATION_OCTET_STREAM, Mime, Name, TEXT_PLAIN};
use crate::rpc::format;
use crate::api::format as api_format;

use crate::api::err::ApiError;
use crate::api::middleware::common::{
	APPLICATION_CBOR, APPLICATION_SDB_FB, APPLICATION_SDB_NATIVE, BodyStrategy,
};
use crate::api::response::ApiResponse;

pub fn output_body_strategy(headers: &HeaderMap, strategy: BodyStrategy) -> Option<BodyStrategy> {
	let Some(accepted) = headers.get(ACCEPT) else {
		// No Accept header was present, we can return the default strategy (decided by the user or
		// otherwise JSON)
		return Some(strategy);
	};

	let accepted = parse_accept(accepted);
	if accepted.is_empty() {
		// The Accept header was present but empty or only contained invalid entries.
		// No output format possible
		return None;
	}

	// Depending on the user chosen strategy, decide which output formats are possible
	let supported: &[_] = match strategy {
		BodyStrategy::Json => &[(BodyStrategy::Json, &APPLICATION_JSON)],
		BodyStrategy::Cbor => &[(BodyStrategy::Cbor, &*APPLICATION_CBOR)],
		BodyStrategy::Flatbuffers => &[(BodyStrategy::Flatbuffers, &*APPLICATION_SDB_FB)],
		BodyStrategy::Plain => &[(BodyStrategy::Plain, &TEXT_PLAIN)],
		BodyStrategy::Bytes => &[(BodyStrategy::Bytes, &APPLICATION_OCTET_STREAM)],
		BodyStrategy::Native => &[(BodyStrategy::Native, &*APPLICATION_SDB_NATIVE)],
		BodyStrategy::Auto => &[
			(BodyStrategy::Json, &APPLICATION_JSON),
			(BodyStrategy::Cbor, &*APPLICATION_CBOR),
			(BodyStrategy::Flatbuffers, &*APPLICATION_SDB_FB),
			(BodyStrategy::Plain, &TEXT_PLAIN),
			(BodyStrategy::Bytes, &APPLICATION_OCTET_STREAM),
			(BodyStrategy::Native, &*APPLICATION_SDB_NATIVE),
		],
	};

	// Check for any matches
	for range in accepted.iter() {
		for (strategy, mime) in supported.iter() {
			if range.matches(mime) {
				return Some(*strategy);
			}
		}
	}

	// No match
	None
}

pub fn convert_response_value(response: &mut ApiResponse, strategy: BodyStrategy) -> Result<()> {
	match strategy {
		BodyStrategy::Auto | BodyStrategy::Json => {
			response.body = PublicValue::Bytes(PublicBytes::from(
				format::json::encode(response.body.clone()).map_err(|_| ApiError::BodyEncodeFailure)?,
			));
			response.headers.insert(CONTENT_TYPE, api_format::JSON.try_into()?);
		}
		BodyStrategy::Cbor => {
			response.body = PublicValue::Bytes(PublicBytes::from(
				format::cbor::encode(response.body.clone()).map_err(|_| ApiError::BodyEncodeFailure)?,
			));
			response.headers.insert(CONTENT_TYPE, api_format::CBOR.try_into()?);
		}
		BodyStrategy::Flatbuffers => {
			response.body = PublicValue::Bytes(PublicBytes::from(
				format::flatbuffers::encode(&response.body).map_err(|_| ApiError::BodyEncodeFailure)?,
			));
			response.headers.insert(CONTENT_TYPE, api_format::FLATBUFFERS.try_into()?);
		}
		BodyStrategy::Bytes => {
			let bytes = convert_public_value_to_internal(response.body.clone())
				.cast_to::<Bytes>()
				.map_err(|_| ApiError::BodyEncodeFailure)?
				.0;
			response.body = PublicValue::Bytes(PublicBytes::from(bytes));
			response.headers.insert(CONTENT_TYPE, api_format::OCTET_STREAM.try_into()?);
		}
		BodyStrategy::Plain => {
			let text = convert_public_value_to_internal(response.body.clone())
				.cast_to::<String>()
				.map_err(|_| ApiError::BodyEncodeFailure)?;
			response.body = PublicValue::Bytes(PublicBytes::from(text.into_bytes()));
			response.headers.insert(CONTENT_TYPE, api_format::PLAIN.try_into()?);
		}
		BodyStrategy::Native => {
			response.headers.insert(CONTENT_TYPE, api_format::NATIVE.try_into()?);
		}
	}

	Ok(())
}

fn parse_accept(value: &HeaderValue) -> Vec<AcceptRange> {
	// Validate that the header value is not empty
	let s = value.to_str().unwrap_or("").trim();
	if s.is_empty() {
		return Vec::new();
	}

	// mime - weight - index
	let mut accepted: Vec<(f32, AcceptRange, usize)> = Vec::new();

	// Split mimes by comma, iterate
	for (i, part) in s.split(',').enumerate() {
		// Check that the part is not empty
		let part = part.trim();
		if part.is_empty() {
			continue;
		}

		let Ok(mime) = Mime::from_str(part) else {
			// invalid mime
			continue;
		};

		// Check for quality factor
		let q = mime
			.get_param("q")
			.and_then(|x: Name<'_>| x.as_str().parse::<f32>().ok())
			.unwrap_or(1.0);

		if q <= 0.0 {
			continue;
		}

		// Convert mime to AcceptRange, and persist
		accepted.push((q, mime.into(), i));
	}

	// Sort by quality factor first, by specifity second, then fallback to positional index when
	// equal
	accepted.sort_by(|a, b| {
		b.0.partial_cmp(&a.0)
			.unwrap_or(Equal)
			.then_with(|| a.1.partial_cmp(&b.1).unwrap_or(Equal))
			.then_with(|| a.2.partial_cmp(&b.2).unwrap_or(Equal))
	});

	accepted.into_iter().map(|x| x.1).collect()
}

#[derive(Clone, Debug)]
enum AcceptRange {
	Exact(Mime),
	TypeWildcard(String),
	Any,
}

impl From<Mime> for AcceptRange {
	fn from(mime: Mime) -> Self {
		if mime.subtype() == mime::STAR {
			match mime.type_() {
				mime::STAR => Self::Any,
				x => Self::TypeWildcard(x.to_string()),
			}
		} else {
			Self::Exact(mime)
		}
	}
}

impl PartialEq for AcceptRange {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Exact(a), Self::Exact(b)) => a.eq(b),
			(Self::TypeWildcard(a), Self::TypeWildcard(b)) => a.eq(b),
			(Self::Any, Self::Any) => true,
			_ => false,
		}
	}
}

impl PartialOrd for AcceptRange {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		self.specifity().partial_cmp(&other.specifity())
	}
}

impl AcceptRange {
	pub(super) fn specifity(&self) -> u8 {
		match self {
			Self::Exact(_) => 0,
			Self::TypeWildcard(_) => 1,
			Self::Any => 2,
		}
	}

	pub(super) fn matches(&self, mime: &Mime) -> bool {
		match self {
			Self::Exact(x) => x.type_() == mime.type_() && x.subtype() == mime.subtype(),
			Self::TypeWildcard(x) => mime.type_().as_str().eq_ignore_ascii_case(x),
			Self::Any => true,
		}
	}
}

#[cfg(test)]
mod tests {
	use http::HeaderMap;
	use http::header::ACCEPT;

	use crate::api::middleware::common::BodyStrategy;
	use crate::api::middleware::res::output_body_strategy;

	macro_rules! case {
		($in:ident => None, $header:expr) => {{
			let mut headers = HeaderMap::new();
			headers.insert(ACCEPT, $header.parse().unwrap());
			// We pass in a header value, so we can safely unwrap here
			let out = output_body_strategy(&headers, BodyStrategy::$in);
			assert!(out.is_none());
		}};
		($in:ident => $out:ident, $header:expr) => {{
			let mut headers = HeaderMap::new();
			headers.insert(ACCEPT, $header.parse().unwrap());
			// We pass in a header value, so we can safely unwrap here
			let out = output_body_strategy(&headers, BodyStrategy::$in).unwrap();
			assert_eq!(out, BodyStrategy::$out);
		}};
	}

	#[test]
	fn tests() {
		// 1) Equal q: exact beats type wildcard even if later
		case!(Auto => Json, "application/*;q=0.9, application/json;q=0.9");

		// 2) Equal q + exact tie: header order breaks tie
		case!(Auto => Plain, "text/plain, application/json");

		// 3) Equal q + exact tie but later is more specific than */*
		case!(Auto => Json, "*/*;q=1.0, application/json;q=1.0");

		// 4) Type wildcard beats */* at same q
		case!(Auto => Plain, "*/*;q=0.9, text/*;q=0.9");

		// 5) Any chooses server preference (first supported in Auto = Json)
		case!(Auto => Json, "*/*");

		// 6) Bytes strategy still negotiates; octet-stream exact wins
		case!(Bytes => Bytes, "application/octet-stream, */*;q=0.1");

		// 7) Strategy-limited: wildcard matches but only one supported
		case!(Plain => Plain, "text/*;q=0.2, application/*;q=0.9");

		// 8) q=0 entries are ignored, so fallback to next match
		case!(Auto => Json, "text/plain;q=0, application/json;q=0.1");

		// 9) Accept params other than q shouldn’t block matching
		case!(Auto => Json, "application/json; charset=utf-8");

		// 10) Case-insensitive type wildcard
		case!(Auto => Plain, "TeXt/*");

		// 11) Duplicate ranges: higher q wins
		case!(Auto => Json, "application/json;q=0.2, application/json;q=0.8");

		// 12) Tie on q + specificity + position: server preference in supported order
		// Here both `application/json` and `application/cbor` have q=1; header order chooses json
		// first.
		case!(Auto => Json, "application/json, application/cbor");

		// 13) Empty accept header value
		case!(Auto => None, "");

		// 14) Only invalid entries
		case!(Auto => None, "not/a-mime, also-bad");

		// 15) Only q<=0 entries (all ignored)
		case!(Auto => None, "application/json;q=0, */*;q=0");
	}
}
