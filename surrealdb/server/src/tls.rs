//! Process-wide TLS crypto provider installation.
//!
//! Centralises the rustls `CryptoProvider` install so every entry point
//! (HTTP/WebSocket server, MCP stdio bridge, Enterprise QUIC cluster bus)
//! agrees on the same provider and so the FIPS feature can be enforced in
//! exactly one place.

use anyhow::Result;
use rustls::crypto::CryptoProvider;

/// Install the rustls process-default crypto provider.
///
/// Under `feature = "fips"` this installs `default_fips_provider()` so TLS
/// handshakes are restricted to FIPS-approved cipher suites, KX groups, and
/// signature schemes, then asserts the resolved provider reports FIPS mode
/// active. Aborts startup if the assertion fails — silent FIPS downgrade is
/// strictly worse than failing closed.
///
/// `install_default` only returns `Err` if another crate has already installed
/// a provider, which leaves a working TLS stack in place, so the install
/// result is intentionally discarded. The FIPS check still runs against
/// whatever provider was installed.
///
/// Idempotent: safe to call from multiple entry points; the first caller wins
/// and subsequent installs are no-ops.
pub fn install_default_crypto_provider() -> Result<()> {
	#[cfg(feature = "fips")]
	let _ = CryptoProvider::install_default(rustls::crypto::default_fips_provider());
	#[cfg(not(feature = "fips"))]
	let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());

	#[cfg(feature = "fips")]
	{
		let provider = CryptoProvider::get_default()
			.ok_or_else(|| anyhow::anyhow!("rustls crypto provider not installed"))?;
		anyhow::ensure!(provider.fips(), "rustls FIPS mode not active");
	}

	Ok(())
}
