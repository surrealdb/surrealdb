use crate::err::Error;

#[cfg(unix)]
use tokio::signal::unix as signal_os;
#[cfg(windows)]
use tokio::signal::windows as signal_os;

pub async fn shutdown_signals() -> Result<String, Error> {
	let mut int_signal = signal_os::signal(signal_os::SignalKind::interrupt())?;
	let mut term_signal = signal_os::signal(signal_os::SignalKind::terminate())?;

	tokio::select! {
        _ = int_signal.recv() => {
            return Ok(String::from("SIGINT"));
        }

        _ = term_signal.recv() => {
            return Ok(String::from("SIGTERM"));
        }
    }
}
