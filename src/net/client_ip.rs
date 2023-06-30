use axum::Extension;
use axum::RequestPartsExt;
use axum::async_trait;
use axum::extract::ConnectInfo;
use axum::extract::FromRef;
use axum::extract::FromRequestParts;
use axum::middleware::Next;
use axum::response::Response;
use clap::ValueEnum;
use http::Request;
use http::StatusCode;
use http::request::Parts;
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
	CfConectingIp,
	/// Fly.io client IP
	#[clap(name = "Fly-Client-IP")]
	FlyClientIp,
	/// Akamai, Cloudflare true client IP
	#[clap(name = "True-Client-IP")]
	TrueClientIP,
	/// Nginx real IP
	#[clap(name = "X-Real-IP")]
	XRealIp,
}

///
/// ClientIP extracts the client IP address from the request.
/// 
/// Example:
/// 
/// ```rust
/// use surrealdb::net::client_ip::ClientIP;
/// use surrealdb::net::AppState;
/// 

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
				if let Ok(ConnectInfo(addr)) = ConnectInfo::<SocketAddr>::from_request_parts(parts, state).await {
					ExtractClientIP(Some(addr.ip().to_string()))
				} else {
					ExtractClientIP(None)
				}
			},
			ClientIp::CfConectingIp => {
				if let Some(ip) = parts.headers.get("Cf-Connecting-IP") {
					ExtractClientIP(Some(ip.to_str().unwrap().to_string()))
				} else {
					ExtractClientIP(None)
				}
			},
			ClientIp::FlyClientIp => {
				if let Some(ip) = parts.headers.get("Fly-Client-IP") {
					ExtractClientIP(Some(ip.to_str().unwrap().to_string()))
				} else {
					ExtractClientIP(None)
				}
			},
			ClientIp::TrueClientIP => {
				if let Some(ip) = parts.headers.get("True-Client-IP") {
					ExtractClientIP(Some(ip.to_str().unwrap().to_string()))
				} else {
					ExtractClientIP(None)
				}
			},
			ClientIp::XRealIp => {
				if let Some(ip) = parts.headers.get("X-Real-IP") {
					ExtractClientIP(Some(ip.to_str().unwrap().to_string()))
				} else {
					ExtractClientIP(None)
				}
			},
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
