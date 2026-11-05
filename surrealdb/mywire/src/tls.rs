//! The TLS a connection can be upgraded to.
//!
//! With the `tls` feature these are `tokio-rustls`'s acceptor and stream. Without it the build
//! links no TLS implementation: [`TlsAcceptor`] and [`TlsStream`] are uninhabited, so
//! `ServeOptions::tls` is always `None`, a client's request for TLS is declined as for a server
//! configured without a certificate, and a host that ends TLS itself hands connections over with
//! `Start::Secured`.

#[cfg(feature = "tls")]
pub use tokio_rustls::TlsAcceptor;
#[cfg(feature = "tls")]
pub use tokio_rustls::server::TlsStream;

#[cfg(not(feature = "tls"))]
pub use self::absent::{TlsAcceptor, TlsStream};

#[cfg(not(feature = "tls"))]
mod absent {
	use std::convert::Infallible;
	use std::marker::PhantomData;
	use std::pin::Pin;
	use std::task::{Context, Poll};

	use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

	/// A TLS acceptor of a build without TLS: there is none.
	pub enum TlsAcceptor {}

	impl TlsAcceptor {
		pub fn accept<IO>(
			&self,
			_stream: IO,
		) -> std::future::Ready<std::io::Result<TlsStream<IO>>> {
			match *self {}
		}
	}

	/// A TLS stream of a build without TLS: there is none.
	pub struct TlsStream<IO> {
		never: Infallible,
		io: PhantomData<IO>,
	}

	impl<IO> TlsStream<IO> {
		pub fn get_ref(&self) -> (&IO, ()) {
			match self.never {}
		}
	}

	impl<IO> AsyncRead for TlsStream<IO> {
		fn poll_read(
			self: Pin<&mut Self>,
			_: &mut Context<'_>,
			_: &mut ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			match self.never {}
		}
	}

	impl<IO> AsyncWrite for TlsStream<IO> {
		fn poll_write(
			self: Pin<&mut Self>,
			_: &mut Context<'_>,
			_: &[u8],
		) -> Poll<std::io::Result<usize>> {
			match self.never {}
		}

		fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			match self.never {}
		}

		fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			match self.never {}
		}
	}
}
