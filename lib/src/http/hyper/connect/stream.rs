use std::pin::Pin;

use hyper::client::connect::Connection;
use tokio::{
	io::{AsyncRead, AsyncWrite},
	net::TcpStream,
};

#[cfg(feature = "native-tls")]
use super::NativeStream;
#[cfg(feature = "rustls")]
use super::RustlsStream;

pub enum Stream {
	Http(TcpStream),
	#[cfg(feature = "rustls")]
	Rustls(RustlsStream),
	#[cfg(feature = "native-tls")]
	Native(NativeStream),
}

impl Connection for Stream {
	fn connected(&self) -> hyper::client::connect::Connected {
		match *self {
			Stream::Http(ref x) => x.connected(),
			#[cfg(feature = "rustls")]
			Stream::Rustls(ref x) => x.get_ref().0.connected(),
			#[cfg(feature = "native-tls")]
			Stream::Native(ref x) => x.connected(),
		}
	}
}

impl AsyncRead for Stream {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		// SAFETY: Pinning is structural for all variants of Stream.
		let this = unsafe { self.get_unchecked_mut() };

		match *this {
			Stream::Http(ref mut x) => Pin::new(x).poll_read(cx, buf),
			#[cfg(feature = "rustls")]
			Stream::Rustls(ref mut x) => Pin::new(x).poll_read(cx, buf),
			#[cfg(feature = "native-tls")]
			Stream::Native(ref mut x) => Pin::new(x).poll_read(cx, buf),
		}
	}
}

impl AsyncWrite for Stream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		// SAFETY: Pinning is structural for all variants of Stream.
		let this = unsafe { self.get_unchecked_mut() };

		match *this {
			Stream::Http(ref mut x) => Pin::new(x).poll_write(cx, buf),
			#[cfg(feature = "rustls")]
			Stream::Rustls(ref mut x) => Pin::new(x).poll_write(cx, buf),
			#[cfg(feature = "native-tls")]
			Stream::Native(ref mut x) => Pin::new(x).poll_write(cx, buf),
		}
	}

	fn poll_flush(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		// SAFETY: Pinning is structural for all variants of Stream.
		let this = unsafe { self.get_unchecked_mut() };

		match *this {
			Stream::Http(ref mut x) => Pin::new(x).poll_flush(cx),
			#[cfg(feature = "rustls")]
			Stream::Rustls(ref mut x) => Pin::new(x).poll_flush(cx),
			#[cfg(feature = "native-tls")]
			Stream::Native(ref mut x) => Pin::new(x).poll_flush(cx),
		}
	}

	fn poll_shutdown(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		// SAFETY: Pinning is structural for all variants of Stream.
		let this = unsafe { self.get_unchecked_mut() };

		match *this {
			Stream::Http(ref mut x) => Pin::new(x).poll_shutdown(cx),
			#[cfg(feature = "rustls")]
			Stream::Rustls(ref mut x) => Pin::new(x).poll_shutdown(cx),
			#[cfg(feature = "native-tls")]
			Stream::Native(ref mut x) => Pin::new(x).poll_shutdown(cx),
		}
	}
}
