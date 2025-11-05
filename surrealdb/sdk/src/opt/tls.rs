/// TLS Configuration
///
/// WARNING: `native-tls` and `rustls` are not stable yet. As we may need to
/// upgrade those dependencies from time to time to keep up with their security
/// fixes, this type is excluded from our stability guarantee.
#[cfg(any(feature = "native-tls", feature = "rustls"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "native-tls", feature = "rustls"))))]
#[derive(Debug, Clone)]
pub enum Tls {
	/// Native TLS configuration
	#[cfg(feature = "native-tls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
	Native(native_tls::TlsConnector),
	/// Rustls configuration
	#[cfg(feature = "rustls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
	Rust(rustls::ClientConfig),
}
