use axum_server::Handle;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{err::Error, rpc, telemetry};

/// Start a graceful shutdown:
/// * Signal the Axum Handle when a shutdown signal is received.
/// * Stop all WebSocket connections.
///
/// A second signal will force an immediate shutdown.
pub fn graceful_shutdown(ct: CancellationToken, http_handle: Handle) -> JoinHandle<()> {
	tokio::spawn(async move {
		let result = listen().await.expect("Failed to listen to shutdown signal");
		info!(target: super::LOG, "{} received. Waiting for graceful shutdown... A second signal will force an immediate shutdown", result);

		tokio::select! {
			// Start a normal graceful shutdown
			_ = async {
				// First stop accepting new HTTP requests
				http_handle.graceful_shutdown(None);

				rpc::graceful_shutdown().await;

				ct.cancel();

				// Flush all telemetry data
				tokio::spawn(async move {
					if let Err(err) = telemetry::shutdown() {
						error!("Failed to flush telemetry data: {}", err);
					}

					info!("Stopped telemetry");
				});
			} => (),
			// Force an immediate shutdown if a second signal is received
			_ = async {
				if let Ok(signal) = listen().await {
					warn!(target: super::LOG, "{} received during graceful shutdown. Terminate immediately...", signal);
				} else {
					error!(target: super::LOG, "Failed to listen to shutdown signal. Terminate immediately...");
				}

				// Force an immediate shutdown
				http_handle.shutdown();

				// Close all WebSocket connections immediately
				rpc::shutdown();
			} => (),
		}
	})
}

#[cfg(unix)]
pub async fn listen() -> Result<String, Error> {
	// Import the OS signals
	use tokio::signal::unix::{signal, SignalKind};
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
pub async fn listen() -> Result<String, Error> {
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
