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

/// Maximum number of bytes held for a line that has not yet been terminated.
///
/// This buffer is host-side heap allocation: it is not part of the WASM guest's
/// linear memory, so it is not counted against the store's memory limiter and
/// is unconstrained by the module's configured memory ceiling. Draining happens
/// on a newline or an explicit flush/shutdown, so a module that writes without
/// ever emitting a newline would grow it without bound and exhaust host memory.
/// On reaching the cap the pending bytes are forwarded as their own chunk, so
/// the buffer is bounded without any output being discarded.
const MAX_PENDING_LINE_BYTES: usize = 1 << 20;

/// Line-buffered [`AsyncWrite`] that forwards complete lines to a [`StdioCallback`].
///
/// A pending, newline-free run is bounded by [`MAX_PENDING_LINE_BYTES`]; see
/// there for why that bound cannot be left to the guest memory limiter.
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

	/// Forwards the pending bytes once they reach [`MAX_PENDING_LINE_BYTES`]
	/// without a newline, bounding the buffer.
	///
	/// The chunk is cut on a UTF-8 character boundary and any trailing
	/// incomplete sequence is left pending, so a multi-byte character spanning
	/// the cut is not turned into a replacement character. A pending run whose
	/// very first byte is invalid has no such boundary; it is released whole so
	/// the bound still holds.
	fn emit_oversized_pending(&mut self) {
		if self.buffer.len() < MAX_PENDING_LINE_BYTES {
			return;
		}
		let split = match std::str::from_utf8(&self.buffer) {
			Ok(_) => self.buffer.len(),
			Err(e) => match e.valid_up_to() {
				0 => self.buffer.len(),
				n => n,
			},
		};
		let cb = self.snapshot_callback();
		let chunk = String::from_utf8_lossy(&self.buffer[..split]);
		cb(&chunk);
		self.buffer.drain(..split);
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
		this.emit_oversized_pending();
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
	use std::sync::Arc;
	use std::task::{Context, Poll, Wake, Waker};

	use parking_lot::Mutex;
	use tokio::io::AsyncWrite;
	use wasmtime_wasi::sockets::SocketAddrUse;

	use super::{CallbackWriter, MAX_PENDING_LINE_BYTES, socket_addr_allowed};
	use crate::net_allow::ResolvedNetAllow;

	struct NoopWake;
	impl Wake for NoopWake {
		fn wake(self: Arc<Self>) {}
	}

	/// A writer whose callback records everything forwarded to it.
	fn writer() -> (CallbackWriter, Arc<Mutex<Vec<String>>>) {
		let sink: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
		let recorder = Arc::clone(&sink);
		let writer = CallbackWriter {
			callback: Arc::new(Mutex::new(Arc::new(move |line: &str| {
				recorder.lock().push(line.to_owned());
			}))),
			buffer: Vec::new(),
		};
		(writer, sink)
	}

	fn write_all(writer: &mut CallbackWriter, chunk: &[u8]) {
		let waker = Waker::from(Arc::new(NoopWake));
		let mut cx = Context::from_waker(&waker);
		match std::pin::Pin::new(writer).poll_write(&mut cx, chunk) {
			Poll::Ready(Ok(n)) => assert_eq!(n, chunk.len()),
			other => panic!("poll_write did not accept the chunk: {other:?}"),
		}
	}

	// The pending buffer is host-side heap allocation outside the guest memory
	// limiter, so a module writing without newlines must not be able to grow it
	// without bound.
	#[test]
	fn newline_free_writes_stay_bounded() {
		let (mut writer, sink) = writer();
		let chunk = vec![b'a'; 64 * 1024];
		// Well past the cap: without the bound the buffer would hold all of it.
		let total = MAX_PENDING_LINE_BYTES * 4;
		let writes = total / chunk.len();
		for _ in 0..writes {
			write_all(&mut writer, &chunk);
			assert!(
				writer.buffer.len() < MAX_PENDING_LINE_BYTES,
				"pending buffer must stay under the cap, got {}",
				writer.buffer.len()
			);
		}
		// Nothing was discarded: every byte written was forwarded or is pending.
		let forwarded: usize = sink.lock().iter().map(|s| s.len()).sum();
		assert_eq!(
			forwarded + writer.buffer.len(),
			total,
			"all written bytes must be forwarded or still pending"
		);
	}

	// Baseline: the ordinary line-buffered path is unchanged, so the bound is
	// not what is doing the draining in normal use.
	#[test]
	fn newline_terminated_writes_drain_per_line() {
		let (mut writer, sink) = writer();
		write_all(&mut writer, b"first\nsecond\n");
		assert!(writer.buffer.is_empty(), "a newline must drain the buffer");
		assert_eq!(*sink.lock(), vec!["first".to_owned(), "second".to_owned()]);
		// A trailing partial line stays pending until flushed.
		write_all(&mut writer, b"third");
		assert_eq!(writer.buffer, b"third");
		writer.flush_remaining();
		assert!(writer.buffer.is_empty());
		assert_eq!(sink.lock().len(), 3);
	}

	// A multi-byte character straddling the cut must not be mangled: the chunk
	// is cut on a character boundary and the incomplete tail stays pending.
	#[test]
	fn oversized_pending_cut_respects_utf8_boundaries() {
		let (mut writer, sink) = writer();
		// Fill to one byte short of the cap, so the next write crosses it.
		write_all(&mut writer, &vec![b'a'; MAX_PENDING_LINE_BYTES - 1]);
		assert!(sink.lock().is_empty(), "nothing forwarded before the cap");
		// Write only the *first* byte of a two-byte character, so the pending
		// run ends mid-character exactly as the cap is reached.
		let e_acute = "é".as_bytes();
		write_all(&mut writer, &e_acute[..1]);
		let forwarded = sink.lock().clone();
		assert_eq!(forwarded.len(), 1, "the cap must forward exactly one chunk");
		assert!(
			!forwarded[0].contains('\u{fffd}'),
			"the chunk must not be cut mid-character into a replacement character"
		);
		assert_eq!(forwarded[0].len(), MAX_PENDING_LINE_BYTES - 1);
		// The incomplete sequence is held so the rest of it can be appended.
		assert_eq!(writer.buffer, &e_acute[..1]);
		write_all(&mut writer, &e_acute[1..]);
		writer.flush_remaining();
		assert_eq!(sink.lock()[1], "é", "the character must survive the split intact");
	}

	// A complete character sitting either side of the cap needs no split: the
	// whole pending run is valid UTF-8 and is forwarded as one chunk.
	#[test]
	fn oversized_pending_forwards_complete_sequences_whole() {
		let (mut writer, sink) = writer();
		write_all(&mut writer, &vec![b'a'; MAX_PENDING_LINE_BYTES - 1]);
		write_all(&mut writer, "é".as_bytes());
		let forwarded = sink.lock().clone();
		assert_eq!(forwarded.len(), 1);
		assert!(forwarded[0].ends_with('é'));
		assert!(writer.buffer.is_empty(), "a fully valid run leaves nothing pending");
	}

	// ---- Socket sandbox filter (`socket_addr_allowed`) ----

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
