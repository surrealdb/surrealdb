use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRef, FromRequestParts, Request};
use axum::middleware::Next;
use axum::response::Response;
use axum::{Extension, RequestPartsExt};
use clap::ValueEnum;
use http::StatusCode;
use http::request::Parts;

use super::AppState;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum ClientIp {
	/// Don't use client IP
	None,
	/// Raw socket IP
	Socket,
	/// Cloudflare connecting IP
	#[clap(name = "CF-Connecting-IP")]
	CfConnectingIp,
	/// Fly.io client IP
	#[clap(name = "Fly-Client-IP")]
	FlyClientIp,
	/// Akamai, Cloudflare true client IP
	#[clap(name = "True-Client-IP")]
	TrueClientIp,
	/// Nginx real IP
	#[clap(name = "X-Real-IP")]
	XRealIp,
	/// Industry standard header used by many proxies
	#[clap(name = "X-Forwarded-For")]
	XForwardedFor,
	/// RFC 7239 `Forwarded` header; the first `for=` parameter is used
	#[clap(name = "Forwarded")]
	Forwarded,
}

impl std::fmt::Display for ClientIp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ClientIp::None => write!(f, "None"),
			ClientIp::Socket => write!(f, "Socket"),
			ClientIp::CfConnectingIp => write!(f, "CF-Connecting-IP"),
			ClientIp::FlyClientIp => write!(f, "Fly-Client-IP"),
			ClientIp::TrueClientIp => write!(f, "True-Client-IP"),
			ClientIp::XRealIp => write!(f, "X-Real-IP"),
			ClientIp::XForwardedFor => write!(f, "X-Forwarded-For"),
			ClientIp::Forwarded => write!(f, "Forwarded"),
		}
	}
}

impl ClientIp {
	fn is_header(self) -> bool {
		match self {
			ClientIp::None => false,
			ClientIp::Socket => false,
			ClientIp::CfConnectingIp => true,
			ClientIp::FlyClientIp => true,
			ClientIp::TrueClientIp => true,
			ClientIp::XRealIp => true,
			ClientIp::XForwardedFor => true,
			ClientIp::Forwarded => true,
		}
	}
}

/// Parses an RFC 7239 `Forwarded` header value and returns the `for=`
/// identifier from the first forwarded element.
///
/// The first element represents the client side of the chain (subsequent
/// elements are added by intermediaries closer to the origin server). The
/// identifier may be quoted; surrounding quotes and backslash escapes are
/// stripped. Returns `None` if the header has no first element or the first
/// element has no `for=` parameter.
fn parse_forwarded_for(header: &str) -> Option<String> {
	// Forwarded grammar (RFC 7239):
	//   Forwarded   = 1#forwarded-element
	//   forwarded-element = [ forwarded-pair ] *( ";" [ forwarded-pair ] )
	//   forwarded-pair    = token "=" value
	//   value             = token / quoted-string
	// Elements are comma-separated, pairs within an element are
	// semicolon-separated. We only care about the first element's `for=`.
	let first_element = split_top_level(header, ',').next()?;
	for pair in split_top_level(first_element, ';') {
		// RFC 7239 allows empty pairs (`;;` or leading/trailing `;`); skip them.
		let Some((name, value)) = pair.trim().split_once('=') else {
			continue;
		};
		if name.trim().eq_ignore_ascii_case("for") {
			return Some(unquote(value.trim()));
		}
	}
	None
}

/// Splits a string on `delim`, ignoring delimiters inside double-quoted
/// segments (with backslash escapes inside the quotes).
fn split_top_level(input: &str, delim: char) -> impl Iterator<Item = &str> {
	let bytes = input.as_bytes();
	let mut start = 0;
	let mut idx = 0;
	let mut in_quotes = false;
	let mut escape = false;
	std::iter::from_fn(move || {
		while idx < bytes.len() {
			let c = bytes[idx] as char;
			if escape {
				escape = false;
			} else if in_quotes {
				match c {
					'\\' => escape = true,
					'"' => in_quotes = false,
					_ => {}
				}
			} else if c == '"' {
				in_quotes = true;
			} else if c == delim {
				let segment = &input[start..idx];
				idx += 1;
				start = idx;
				return Some(segment);
			}
			idx += 1;
		}
		if start <= bytes.len() {
			let segment = &input[start..bytes.len()];
			start = bytes.len() + 1;
			Some(segment)
		} else {
			None
		}
	})
}

/// Strips surrounding double quotes from a value and unescapes backslash
/// escapes inside, per RFC 7230 quoted-string. Non-quoted values are
/// returned unchanged.
fn unquote(value: &str) -> String {
	let bytes = value.as_bytes();
	if bytes.len() < 2 || bytes[0] != b'"' || bytes[bytes.len() - 1] != b'"' {
		return value.to_owned();
	}
	let inner = &value[1..value.len() - 1];
	let mut out = String::with_capacity(inner.len());
	let mut escape = false;
	for c in inner.chars() {
		if escape {
			out.push(c);
			escape = false;
		} else if c == '\\' {
			escape = true;
		} else {
			out.push(c);
		}
	}
	out
}

#[derive(Clone)]
pub(super) struct ExtractClientIP(pub Option<String>);

impl<S> FromRequestParts<S> for ExtractClientIP
where
	AppState: FromRef<S>,
	S: Send + Sync,
{
	type Rejection = (StatusCode, &'static str);

	async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
		let app_state = AppState::from_ref(state);

		let res = match app_state.client_ip {
			ClientIp::None => ExtractClientIP(None),
			ClientIp::Socket => {
				match ConnectInfo::<SocketAddr>::from_request_parts(parts, state).await {
					Ok(ConnectInfo(addr)) => ExtractClientIP(Some(addr.ip().to_string())),
					_ => ExtractClientIP(None),
				}
			}
			// Get the IP from the corresponding header
			var if var.is_header() => {
				if let Some(ip) = parts.headers.get(var.to_string()) {
					match ip.to_str() {
						Ok(s) => {
							let parsed = match var {
								ClientIp::Forwarded => parse_forwarded_for(s),
								_ => Some(s.to_string()),
							};
							ExtractClientIP(parsed)
						}
						Err(err) => {
							debug!("Invalid header value for {}: {}", var, err);
							ExtractClientIP(None)
						}
					}
				} else {
					ExtractClientIP(None)
				}
			}
			_ => {
				warn!("Unexpected ClientIp variant: {:?}", app_state.client_ip);
				ExtractClientIP(None)
			}
		};

		Ok(res)
	}
}

pub(super) async fn client_ip_middleware(
	request: Request,
	next: Next,
) -> Result<Response, StatusCode> {
	let (mut parts, body) = request.into_parts();

	match parts.extract::<Extension<AppState>>().await {
		Ok(Extension(state)) => {
			if let Ok(client_ip) =
				parts.extract_with_state::<ExtractClientIP, AppState>(&state).await
			{
				parts.extensions.insert(client_ip);
			}
		}
		_ => {
			trace!("No AppState found, skipping client_ip_middleware");
		}
	}

	Ok(next.run(Request::from_parts(parts, body)).await)
}

#[cfg(test)]
mod tests {
	use super::parse_forwarded_for;

	#[test]
	fn forwarded_simple() {
		assert_eq!(parse_forwarded_for("for=192.0.2.43"), Some("192.0.2.43".to_owned()));
	}

	#[test]
	fn forwarded_takes_first_element() {
		assert_eq!(
			parse_forwarded_for("for=192.0.2.43, for=198.51.100.17"),
			Some("192.0.2.43".to_owned())
		);
	}

	#[test]
	fn forwarded_skips_other_parameters() {
		assert_eq!(
			parse_forwarded_for("by=203.0.113.43;proto=http;for=192.0.2.60"),
			Some("192.0.2.60".to_owned())
		);
	}

	#[test]
	fn forwarded_is_case_insensitive() {
		assert_eq!(parse_forwarded_for("For=192.0.2.60"), Some("192.0.2.60".to_owned()));
		assert_eq!(parse_forwarded_for("FOR=192.0.2.60"), Some("192.0.2.60".to_owned()));
	}

	#[test]
	fn forwarded_ipv6_quoted() {
		assert_eq!(
			parse_forwarded_for(r#"for="[2001:db8:cafe::17]:4711""#),
			Some("[2001:db8:cafe::17]:4711".to_owned())
		);
	}

	#[test]
	fn forwarded_quoted_with_escape() {
		assert_eq!(parse_forwarded_for(r#"for="\"weird\"""#), Some(r#""weird""#.to_owned()));
	}

	#[test]
	fn forwarded_quoted_value_with_semicolon() {
		assert_eq!(
			parse_forwarded_for(r#"for="192.0.2.43;not-a-param";by=203.0.113.43"#),
			Some("192.0.2.43;not-a-param".to_owned())
		);
	}

	#[test]
	fn forwarded_quoted_value_with_comma() {
		assert_eq!(
			parse_forwarded_for(r#"for="192.0.2.43,still-first", for=198.51.100.17"#),
			Some("192.0.2.43,still-first".to_owned())
		);
	}

	#[test]
	fn forwarded_obfuscated_identifier() {
		// RFC 7239 §6.3: obfuscated identifiers begin with `_`.
		assert_eq!(parse_forwarded_for("for=_hidden"), Some("_hidden".to_owned()));
	}

	#[test]
	fn forwarded_unknown_identifier() {
		// RFC 7239 §6.1: unknown sources are represented as `unknown`.
		assert_eq!(parse_forwarded_for("for=unknown"), Some("unknown".to_owned()));
	}

	#[test]
	fn forwarded_no_for_parameter() {
		assert_eq!(parse_forwarded_for("by=203.0.113.43;proto=http"), None);
	}

	#[test]
	fn forwarded_empty() {
		assert_eq!(parse_forwarded_for(""), None);
	}

	#[test]
	fn forwarded_skips_empty_pairs() {
		// RFC 7239 forwarded-element allows empty pairs (leading/trailing/repeated `;`).
		assert_eq!(
			parse_forwarded_for(";by=203.0.113.43;;for=192.0.2.60;"),
			Some("192.0.2.60".to_owned())
		);
	}
}
