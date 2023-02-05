use crate::err::Error;

#[cfg(unix)]
use tokio::signal::unix as signal_os;
#[cfg(windows)]
use tokio::signal::windows as signal_os;

pub async fn listen() -> Result<String, Error> {
	// Get the operating system signal types
	let mut interrupt = signal_os::signal(signal_os::SignalKind::interrupt())?;
	let mut terminate = signal_os::signal(signal_os::SignalKind::terminate())?;
	// Wait until we receive a shutdown signal
	tokio::select! {
		// Wait for an interrupt signal
		_ = interrupt.recv() => {
			Ok(String::from("SIGINT"))
		}
		// Wait for a terminate signal
		_ = terminate.recv() => {
			Ok(String::from("SIGTERM"))
		}
	}
}
