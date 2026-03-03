use std::collections::HashSet;
use std::sync::Arc;

use async_channel::Receiver;
use reqwest::ClientBuilder;
use surrealdb_core::cnf::HttpClientConfig;
use tokio::sync::watch;
use url::Url;

use super::{Client, RouterState};
use crate::conn::{Route, Router};
use crate::engine::{SessionError, session_error_to_error};
use crate::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::opt::Tls;
use crate::opt::{Endpoint, WaitFor};
use crate::{Error, ExtraFeatures, Result, SessionClone, SessionId, Surreal, conn};

/// Creates an HTTP client with address pinning for the given URL.
///
/// This function resolves the hostname to IP addresses and tries each one
/// until a successful health check is performed. The resulting client is
/// configured with `reqwest::ClientBuilder::resolve()` to pin all requests
/// to the working IP address, ensuring session consistency.
///
/// # Arguments
///
/// * `base_url` - The base URL of the SurrealDB server
/// * `tls_config` - Optional TLS configuration for HTTPS connections
///
/// # Returns
///
/// A configured `reqwest::Client` pinned to a specific server IP address.
pub(crate) async fn create_client(
	base_url: &Url,
	#[cfg(any(feature = "native-tls", feature = "rustls"))] tls_config: Option<&Tls>,
) -> Result<reqwest::Client> {
	let headers = super::default_headers();

	// Extract hostname and port for DNS resolution
	let hostname = base_url.domain().unwrap_or("localhost");
	let port = base_url.port_or_known_default().unwrap_or(8000);

	// Resolve hostname to get list of addresses
	let addrs = tokio::net::lookup_host((hostname, port)).await.map_err(|error| {
		Error::internal(format!("DNS resolution failed for {hostname}:{port}; {error}"))
	})?;

	// Try each address until one works
	let mut last_error = None;

	for addr in addrs {
		#[cfg_attr(not(any(feature = "native-tls", feature = "rustls")), expect(unused_mut))]
		let mut builder = ClientBuilder::new().default_headers(headers.clone()).resolve(hostname, addr);

		#[cfg(any(feature = "native-tls", feature = "rustls"))]
		if let Some(tls) = tls_config {
			builder = match tls {
				#[cfg(feature = "native-tls")]
				Tls::Native(config) => builder.use_preconfigured_tls(config.clone()),
				#[cfg(feature = "rustls")]
				Tls::Rust(config) => builder.use_preconfigured_tls(config.clone()),
			};
		}

		let client = match builder.build() {
			Ok(client) => client,
			Err(error) => {
				last_error = Some(Error::internal(error.to_string()));
				continue;
			}
		};

		// Try health check with this address
		let req = client
			.get(base_url.join("health").map_err(crate::std_error_to_types_error)?)
			.header(reqwest::header::USER_AGENT, HttpClientConfig::default().user_agent.as_str());

		match super::health(req).await {
			Ok(()) => return Ok(client),
			Err(e) => {
				last_error = Some(e);
				continue;
			}
		}
	}

	Err(last_error.unwrap_or_else(|| Error::internal("No addresses available".to_string())))
}

impl crate::Connection for Client {}
impl conn::Sealed for Client {
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let config = address.config.clone();
			let base_url = address.url;

			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			let client = create_client(&base_url, address.config.tls_config.as_ref()).await?;
			#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
			let client = create_client(&base_url).await?;

			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			tokio::spawn(run_router(client, base_url, route_rx, session_clone.receiver.clone()));

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}

pub(crate) async fn run_router(
	client: reqwest::Client,
	base_url: url::Url,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	let state = Arc::new(RouterState::new(client, base_url));
	loop {
		tokio::select! {
			biased;

			session = session_rx.recv() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
					SessionId::Initial(session_id) => {
						state.handle_session_initial(session_id).await;
					}
					SessionId::Clone { old, new } => {
						state.handle_session_clone(old, new).await;
					}
					SessionId::Drop(session_id) => {
						state.handle_session_drop(session_id).await;
					}
				}
			}
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};

				let session_id = route.request.session_id;
				let command = route.request.command.clone();

				// Get session state for this session_id
				let session_state = match state.sessions.get(&session_id) {
					Some(Ok(state)) => state,
					Some(Err(error)) => {
						route.response.send(Err(session_error_to_error(error))).await.ok();
						continue;
					}
					None => {
						let error = session_error_to_error(SessionError::NotFound(session_id));
						route.response.send(Err(error)).await.ok();
						continue;
					}
				};

				// Spawn the request handling in a background task
				// SessionState uses RwLock internally, so we can share the Arc directly
				let router_state = state.clone();
				tokio::spawn(async move {
					let result = super::router(
						route.request,
						&router_state.base_url,
						&router_state.client,
						&session_state,
					)
					.await;

					// On success, add replayable commands to the replay list
					// boxcar::Vec is lock-free, so this is safe to do concurrently
					if result.is_ok() && command.replayable() {
						session_state.replay.push(command);
					}

					// Convert api::err::Error to wire error type
					let db_result = result;
					route.response.send(db_result).await.ok();
				});
			}
		}
	}
}
