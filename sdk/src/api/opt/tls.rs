/// TLS Configuration
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
