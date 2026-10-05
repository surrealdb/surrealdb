use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use parking_lot::Mutex;
use surrealism_types::err::PrefixErr;
use tokio::io::AsyncWrite;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::sockets::SocketAddrUse;
use wasmtime_wasi::{FsPerms, WasiCtx, WasiCtxBuilder};

use crate::net_allow::ResolvedNetAllow;

/// Shared swappable callback for forwarding WASI stdio output.
///
/// Held by both the WASI output stream (which calls it on writes) and the
/// controller (which swaps it when the invocation context changes).
///
/// The inner `Arc<dyn Fn>` allows writers to snapshot the callback with a
/// cheap `Arc::clone` and release the lock before invoking it, so the
/// mutex is never held during potentially expensive I/O (tracing, log
/// formatting, etc.).
pub type StdioCallback = Arc<Mutex<Arc<dyn Fn(&str) + Send + Sync>>>;

pub fn new_stdout_callback() -> StdioCallback {
	Arc::new(Mutex::new(Arc::new(|output| print!("{}", output))))
}

pub fn new_stderr_callback() -> StdioCallback {
	Arc::new(Mutex::new(Arc::new(|output| eprint!("{}", output))))
}

/// A [`StdoutStream`] backed by a shared [`StdioCallback`].
///
/// Bytes are line-buffered: complete lines are forwarded to the callback
/// as they arrive, and any trailing partial line is flushed on shutdown.
struct CallbackStdoutStream {
	callback: StdioCallback,
}

impl IsTerminal for CallbackStdoutStream {
	fn is_terminal(&self) -> bool {
		false
	}
}

impl StdoutStream for CallbackStdoutStream {
	fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
		Box::new(CallbackWriter {
			callback: Arc::clone(&self.callback),
			buffer: Vec::new(),
		})
	}
}

/// Line-buffered [`AsyncWrite`] that forwards complete lines to a [`StdioCallback`].
struct CallbackWriter {
	callback: StdioCallback,
	buffer: Vec<u8>,
}

impl CallbackWriter {
	/// Snapshot the current callback so the mutex is not held during invocation.
	fn snapshot_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		Arc::clone(&self.callback.lock())
	}

	fn emit_lines(&mut self) {
		let cb = self.snapshot_callback();
		while let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
			let line = String::from_utf8_lossy(&self.buffer[..pos]);
			cb(&line);
			self.buffer.drain(..=pos);
		}
	}

	fn flush_remaining(&mut self) {
		if !self.buffer.is_empty() {
			let cb = self.snapshot_callback();
			let remaining = String::from_utf8_lossy(&self.buffer);
			cb(&remaining);
			self.buffer.clear();
		}
	}
}

impl AsyncWrite for CallbackWriter {
	fn poll_write(
		self: Pin<&mut Self>,
		_cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		let this = self.get_mut();
		this.buffer.extend_from_slice(buf);
		this.emit_lines();
		Poll::Ready(Ok(buf.len()))
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.get_mut().flush_remaining();
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.get_mut().flush_remaining();
		Poll::Ready(Ok(()))
	}
}

/// Decide whether a WASI socket operation is permitted for a networked module.
///
/// The only capability granted is outbound TCP to addresses the `allow_net`
/// filters admit: `TcpConnect` is gated against the filters. Connecting an unbound
/// socket makes WASI check an implicit bind to the unspecified address with an
/// ephemeral port first; that bind carries no remote target, so it is allowed and
/// the real decision is taken on the connect address. A concrete local bind,
/// listening, accepting, and every UDP operation are refused, so a networked
/// module gets outbound TCP to allow-listed hosts and nothing else.
fn socket_addr_allowed(
	reason: SocketAddrUse,
	addr: &SocketAddr,
	filters: &[ResolvedNetAllow],
) -> bool {
	match reason {
		SocketAddrUse::TcpBind => addr.ip().is_unspecified() && addr.port() == 0,
		SocketAddrUse::TcpConnect => filters.iter().any(|f| f.matches_socket_addr(addr)),
		_ => false,
	}
}

pub fn build(
	fs_root: Option<&Path>,
	allow_net: Arc<Vec<ResolvedNetAllow>>,
	stdout_cb: StdioCallback,
	stderr_cb: StdioCallback,
) -> Result<(WasiCtx, ResourceTable)> {
	let mut builder = WasiCtxBuilder::new();
	builder.stdout(CallbackStdoutStream {
		callback: stdout_cb,
	});
	builder.stderr(CallbackStdoutStream {
		callback: stderr_cb,
	});

	if allow_net.is_empty() {
		builder.allow_tcp(false);
		builder.allow_udp(false);
		builder.allow_ip_name_lookup(false);
	} else {
		// wasmtime-wasi denies every socket by default, so TCP has to be switched
		// on explicitly; each connection is still gated by the `socket_addr_check`
		// below. UDP is left off, so modules get outbound TCP only.
		builder.allow_tcp(true);
		builder.allow_udp(false);
		// Hostnames are resolved at module load in `net_allow`, so guests
		// don't need runtime DNS. Disabling it prevents DNS tunneling.
		builder.allow_ip_name_lookup(false);
		let filters = allow_net;
		builder.socket_addr_check(move |addr, reason| {
			let allowed = socket_addr_allowed(reason, &addr, &filters);
			Box::pin(async move { allowed })
		});
	}

	if let Some(root) = fs_root {
		builder
			.preopened_dir(root, "/", FsPerms::ReadOnly)
			.prefix_err(|| "Failed to preopen filesystem directory")?;
	}
	Ok((builder.build(), ResourceTable::new()))
}

#[cfg(test)]
mod tests {
	use std::net::SocketAddr;

	use wasmtime_wasi::sockets::SocketAddrUse;

	use super::socket_addr_allowed;
	use crate::net_allow::ResolvedNetAllow;

	fn allow_filters() -> Vec<ResolvedNetAllow> {
		vec![ResolvedNetAllow::IpPort("1.2.3.4".parse().unwrap(), 443)]
	}

	fn sa(s: &str) -> SocketAddr {
		s.parse().unwrap()
	}

	#[test]
	fn tcp_connect_is_gated_by_the_filters() {
		let f = allow_filters();
		assert!(socket_addr_allowed(SocketAddrUse::TcpConnect, &sa("1.2.3.4:443"), &f));
		// A different port or host is refused.
		assert!(!socket_addr_allowed(SocketAddrUse::TcpConnect, &sa("1.2.3.4:80"), &f));
		assert!(!socket_addr_allowed(SocketAddrUse::TcpConnect, &sa("5.6.7.8:443"), &f));
	}

	#[test]
	fn implicit_bind_to_unspecified_is_allowed() {
		// Connecting an unbound socket makes WASI check an implicit bind to the
		// unspecified address first; denying it would break every outbound
		// connect, so it must be allowed regardless of the filters.
		let f = allow_filters();
		assert!(socket_addr_allowed(SocketAddrUse::TcpBind, &sa("0.0.0.0:0"), &f));
		assert!(socket_addr_allowed(SocketAddrUse::TcpBind, &sa("[::]:0"), &f));
	}

	#[test]
	fn concrete_bind_listen_and_accept_are_denied() {
		let f = allow_filters();
		// A concrete local bind is not the implicit connect-bind, even to an
		// address the filters would allow as a connect target.
		assert!(!socket_addr_allowed(SocketAddrUse::TcpBind, &sa("1.2.3.4:443"), &f));
		// The unspecified address with a fixed (non-ephemeral) port is a real
		// bind, not the implicit connect-bind, so it is refused too.
		assert!(!socket_addr_allowed(SocketAddrUse::TcpBind, &sa("0.0.0.0:8080"), &f));
		assert!(!socket_addr_allowed(SocketAddrUse::TcpListen, &sa("0.0.0.0:0"), &f));
		assert!(!socket_addr_allowed(SocketAddrUse::TcpAccept, &sa("1.2.3.4:443"), &f));
	}

	#[test]
	fn all_udp_is_denied() {
		// UDP stays off entirely, even to a filter-allowed address.
		let f = allow_filters();
		assert!(!socket_addr_allowed(SocketAddrUse::UdpBind, &sa("0.0.0.0:0"), &f));
		assert!(!socket_addr_allowed(SocketAddrUse::UdpSend, &sa("1.2.3.4:443"), &f));
		assert!(!socket_addr_allowed(SocketAddrUse::UdpReceive, &sa("1.2.3.4:443"), &f));
	}
}
