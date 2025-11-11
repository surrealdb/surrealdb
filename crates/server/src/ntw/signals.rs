use std::sync::Arc;

use anyhow::Result;
use axum_server::Handle;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::rpc::{self, RpcState};
use crate::telemetry;

/// Start a graceful shutdown:
/// * Signal the Axum Handle when a shutdown signal is received.
/// * Stop all WebSocket connections.
/// * Flush all telemetry data.
///
/// A second signal will force an immediate shutdown.
pub fn graceful_shutdown(
	state: Arc<RpcState>,
	canceller: CancellationToken,
	http_handle: Handle,
) -> JoinHandle<()> {
	// Spawn a new background asynchronous task
	tokio::spawn(async move {
		// Listen to the primary OS task signal
		match listen().await {
			Ok(signal) => {
				warn!(target: super::LOG, "{signal} received. Waiting for a graceful shutdown. A second signal will force an immediate shutdown.");
			}
			_ => {
				error!(target: super::LOG, "Failed to listen to shutdown signal. Terminating immediately.");
				canceller.cancel();
			}
		}
		// Spawn a task to gracefully shutdown
		let shutdown = {
			// Clone the state
			let http_handle = http_handle.clone();
			let canceller = canceller.clone();
			let state = state.clone();
			// Spawn a background task
			tokio::spawn(async move {
				// Stop accepting new HTTP connections
				http_handle.graceful_shutdown(None);
				// Wait for all connections to close
				while http_handle.connection_count() > 0 {
					tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
				}
				// Stop accepting new WebSocket connections
				rpc::graceful_shutdown(state).await;
				// Cancel the cancellation token
				canceller.cancel();
				// Flush all telemetry data
				telemetry::shutdown();
			})
		};
		// Wait for the primary or secondary signals to complete
		tokio::select! {
			// Check signals in order
			biased;
			// Start a normal graceful shutdown
			_ = shutdown => (),
			// Check if this has shutdown
			_ = canceller.cancelled() => {
				// Close all HTTP connections immediately
				http_handle.shutdown();
				// Close all WebSocket connections immediately
				rpc::shutdown(state);
				// Cancel the cancellation token
				canceller.cancel();
				// Flush all telemetry data
				telemetry::shutdown();
			}
			// Listen for a secondary signal
			res = listen() => {
				// If we receive a secondary signal, force a shutdown
				if let Ok(signal) = res {
					warn!(target: super::LOG, "{signal} received during graceful shutdown. Terminating immediately.");
				} else {
					error!(target: super::LOG, "Failed to listen to shutdown signal. Terminating immediately.");
				}
				// Close all HTTP connections immediately
				http_handle.shutdown();
				// Close all WebSocket connections immediately
				rpc::shutdown(state);
				// Cancel the cancellation token
				canceller.cancel();
				// Flush all telemetry data
				telemetry::shutdown();
			},
		}
	})
}

#[cfg(unix)]
pub async fn listen() -> Result<String> {
	// Log informational message
	info!(target: super::LOG, "Listening for a system shutdown signal.");
	// Import the OS signals
	use tokio::signal::unix::{SignalKind, signal};
	// Get the operating system signal types
	let mut sighup = signal(SignalKind::hangup())?;
	let mut sigint = signal(SignalKind::interrupt())?;
	let mut sigquit = signal(SignalKind::quit())?;
	let mut sigterm = signal(SignalKind::terminate())?;
	// Listen and wait for the system signals
	tokio::select! {
		// Wait for a SIGHUP signal
		_ = sighup.recv() => {
			Ok(String::from("SIGHUP"))
		}
		// Wait for a SIGINT signal
		_ = sigint.recv() => {
			Ok(String::from("SIGINT"))
		}
		// Wait for a SIGQUIT signal
		_ = sigquit.recv() => {
			Ok(String::from("SIGQUIT"))
		}
		// Wait for a SIGTERM signal
		_ = sigterm.recv() => {
			Ok(String::from("SIGTERM"))
		}
	}
}

#[cfg(windows)]
pub async fn listen() -> Result<String> {
	// Log informational message
	info!(target: super::LOG, "Listening for a system shutdown signal.");
	// Import the OS signals
	use tokio::signal::windows;
	// Get the operating system signal types
	let mut exit = windows::ctrl_c()?;
	let mut leave = windows::ctrl_break()?;
	let mut close = windows::ctrl_close()?;
	let mut shutdown = windows::ctrl_shutdown()?;
	// Listen and wait for the system signals
	tokio::select! {
		// Wait for a CTRL-C signal
		_ = exit.recv() => {
			Ok(String::from("CTRL-C"))
		}
		// Wait for a CTRL-BREAK signal
		_ = leave.recv() => {
			Ok(String::from("CTRL-BREAK"))
		}
		// Wait for a CTRL-CLOSE signal
		_ = close.recv() => {
			Ok(String::from("CTRL-CLOSE"))
		}
		// Wait for a CTRL-SHUTDOWN signal
		_ = shutdown.recv() => {
			Ok(String::from("CTRL-SHUTDOWN"))
		}
	}
}
