use crate::http::Uri;
use hyper::{client::HttpConnector, service::Service};

use std::{
	error::Error as StdError,
	future::Future,
	pin::Pin,
	task::{ready, Poll},
};

#[cfg(feature = "native-tls")]
use super::NativeConnector;
#[cfg(feature = "rustls")]
use super::RustlsConnector;
use super::Stream;

/// A connector future with support for different TLS connections.
pub enum ConnectorFuture {
	#[cfg(feature = "rustls")]
	Rustls {
		inner: <RustlsConnector as Service<Uri>>::Future,
	},
	#[cfg(feature = "native-tls")]
	Native {
		inner: <NativeConnector as Service<Uri>>::Future,
	},
	Http {
		inner: <HttpConnector as Service<Uri>>::Future,
	},
}

impl Future for ConnectorFuture {
	type Output = Result<Stream, Box<dyn StdError + Send + Sync>>;

	fn poll(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Self::Output> {
		// SAFETY: For ConnectorFuture pinning is structural for all variants and fields.
		let inner = unsafe { self.get_unchecked_mut() };
		match inner {
			#[cfg(feature = "rustls")]
			ConnectorFuture::Rustls {
				ref mut inner,
			} => {
				let res = ready!(Pin::new(inner).poll(cx));
				match res {
					Err(e) => Poll::Ready(Err(e)),
					Ok(hyper_rustls::MaybeHttpsStream::Http(x)) => Poll::Ready(Ok(Stream::Http(x))),
					Ok(hyper_rustls::MaybeHttpsStream::Https(x)) => {
						Poll::Ready(Ok(Stream::Rustls(x)))
					}
				}
			}
			#[cfg(feature = "native-tls")]
			ConnectorFuture::Native {
				ref mut inner,
			} => {
				let res = ready!(Pin::new(inner).poll(cx));
				match res {
					Err(e) => Poll::Ready(Err(e)),
					Ok(hyper_rustls::MaybeHttpsStream::Http(x)) => Poll::Ready(Ok(Stream::Http(x))),
					Ok(hyper_rustls::MaybeHttpsStream::Https(x)) => {
						Poll::Ready(Ok(Stream::Native(x)))
					}
				}
			}
			ConnectorFuture::Http {
				ref mut inner,
			} => {
				let res = ready!(Pin::new(inner).poll(cx));
				match res {
					Ok(x) => Poll::Ready(Ok(Stream::Http(x))),
					Err(e) => Poll::Ready(Err(Box::new(e) as Box<dyn StdError + Send + Sync>)),
				}
			}
		}
	}
}
