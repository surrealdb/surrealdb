use axum::async_trait;
use axum::extract::ConnectInfo;
use axum::extract::FromRef;
use axum::extract::FromRequestParts;
use axum::middleware::Next;
use axum::response::Response;
use axum::Extension;
use axum::RequestPartsExt;
use clap::ValueEnum;
use http::request::Parts;
use http::Request;
use http::StatusCode;
use std::net::SocketAddr;

use super::AppState;

// TODO: Support Forwarded, X-Forwarded-For headers.
// Get inspiration from https://github.com/imbolc/axum-client-ip or simply use it
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
	#[allow(clippy::enum_variant_names)]
	FlyClientIp,
	/// Akamai, Cloudflare true client IP
	#[clap(name = "True-Client-IP")]
	#[allow(clippy::enum_variant_names)]
	TrueClientIP,
	/// Nginx real IP
	#[clap(name = "X-Real-IP")]
	XRealIp,
	/// Industry standard header used by many proxies
	#[clap(name = "X-Forwarded-For")]
	XForwardedFor,
}

impl std::fmt::Display for ClientIp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ClientIp::None => write!(f, "None"),
			ClientIp::Socket => write!(f, "Socket"),
			ClientIp::CfConnectingIp => write!(f, "CF-Connecting-IP"),
			ClientIp::FlyClientIp => write!(f, "Fly-Client-IP"),
			ClientIp::TrueClientIP => write!(f, "True-Client-IP"),
			ClientIp::XRealIp => write!(f, "X-Real-IP"),
			ClientIp::XForwardedFor => write!(f, "X-Forwarded-For"),
		}
	}
}

impl ClientIp {
	fn is_header(&self) -> bool {
		match self {
			ClientIp::None => false,
			ClientIp::Socket => false,
			ClientIp::CfConnectingIp => true,
			ClientIp::FlyClientIp => true,
			ClientIp::TrueClientIP => true,
			ClientIp::XRealIp => true,
			ClientIp::XForwardedFor => true,
		}
	}
}

pub(super) struct ExtractClientIP(pub Option<String>);

#[async_trait]
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
				if let Ok(ConnectInfo(addr)) =
					ConnectInfo::<SocketAddr>::from_request_parts(parts, state).await
				{
					ExtractClientIP(Some(addr.ip().to_string()))
				} else {
					ExtractClientIP(None)
				}
			}
			// Get the IP from the corresponding header
			var if var.is_header() => {
				if let Some(ip) = parts.headers.get(var.to_string()) {
					ip.to_str().map(|s| ExtractClientIP(Some(s.to_string()))).unwrap_or_else(
						|err| {
							debug!("Invalid header value for {}: {}", var, err);
							ExtractClientIP(None)
						},
					)
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

pub(super) async fn client_ip_middleware<B>(
	request: Request<B>,
	next: Next<B>,
) -> Result<Response, StatusCode>
where
	B: Send,
{
	let (mut parts, body) = request.into_parts();

	if let Ok(Extension(state)) = parts.extract::<Extension<AppState>>().await {
		if let Ok(client_ip) = parts.extract_with_state::<ExtractClientIP, AppState>(&state).await {
			parts.extensions.insert(client_ip);
		}
	} else {
		trace!("No AppState found, skipping client_ip_middleware");
	}

	Ok(next.run(Request::from_parts(parts, body)).await)
}
