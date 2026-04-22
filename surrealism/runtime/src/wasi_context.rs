use std::io;
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
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder};

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
			callback: self.callback.clone(),
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
		// Hostnames are resolved at module load in `net_allow`, so guests
		// don't need runtime DNS. Disabling it prevents DNS tunneling.
		builder.allow_ip_name_lookup(false);
		let filters = allow_net;
		builder.socket_addr_check(move |addr, reason| {
			let is_outbound = matches!(
				reason,
				SocketAddrUse::TcpConnect
					| SocketAddrUse::UdpConnect
					| SocketAddrUse::UdpOutgoingDatagram
			);
			let allowed = is_outbound && filters.iter().any(|f| f.matches_socket_addr(&addr));
			Box::pin(async move { allowed })
		});
	}

	if let Some(root) = fs_root {
		builder
			.preopened_dir(root, "/", DirPerms::READ, FilePerms::READ)
			.prefix_err(|| "Failed to preopen filesystem directory")?;
	}
	Ok((builder.build(), ResourceTable::new()))
}
