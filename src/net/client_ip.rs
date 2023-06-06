use crate::cli::CF;
use clap::ValueEnum;
use std::net::IpAddr;
use std::net::SocketAddr;
use warp::Filter;

// TODO: Support Forwarded, X-Forwarded-For headers.
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

/// Creates an string represenation of the client's IP address
pub fn build() -> impl Filter<Extract = (Option<String>,), Error = warp::Rejection> + Clone {
	// Get configured client IP source
	let client_ip = CF.get().unwrap().client_ip;
	// Enable on any path
	let conf = warp::any();
	// Add raw remote IP address
	let conf =
		conf.and(warp::filters::addr::remote().and_then(move |s: Option<SocketAddr>| async move {
			match client_ip {
				ClientIp::None => Ok(None),
				ClientIp::Socket => Ok(s.map(|s| s.ip())),
				// Move on to parsing selected IP header.
				_ => Err(warp::reject::reject()),
			}
		}));
	// Add selected IP header
	let conf = conf.or(warp::header::optional::<IpAddr>(match client_ip {
		ClientIp::CfConectingIp => "Cf-Connecting-IP",
		ClientIp::FlyClientIp => "Fly-Client-IP",
		ClientIp::TrueClientIP => "True-Client-IP",
		ClientIp::XRealIp => "X-Real-IP",
		// none and socket are already handled so this will never be used
		_ => "unreachable",
	}));
	// Join the two filters
	conf.unify().map(|ip: Option<IpAddr>| ip.map(|ip| ip.to_string()))
}
