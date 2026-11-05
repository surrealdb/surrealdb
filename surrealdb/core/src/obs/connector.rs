//! The HTTP connector every `object_store` cloud client in this crate is
//! built with.

use object_store::ClientOptions;
use object_store::client::{HttpClient, HttpConnector, ReqwestConnector};

/// Builds `object_store`'s default reqwest client, trusting the Mozilla root
/// certificates bundled by `webpki-root-certs` alongside the system trust
/// store.
///
/// `object_store` verifies server certificates with `rustls-platform-verifier`,
/// which on Linux reads only the system store and refuses to build a client
/// when that store is empty. Merging the bundled roots keeps S3, GCS and Azure
/// endpoints reachable from images that ship no CA bundle, while a private CA
/// installed in the system store is still trusted.
///
/// Every option the builder resolved, from the URL, the environment or its
/// setters, reaches the client unchanged; the connector only adds root
/// certificates. It adds none when:
/// - the options set `no_system_certificates` to anything but a false value: the caller asked to
///   trust only the certificates it registered, and the bundled roots would widen that set. An
///   unparseable value is passed through for `object_store` to reject as it otherwise would;
/// - the target is Android, where reqwest refuses extra roots and the platform verifier always has
///   the system store.
#[derive(Debug, Default)]
pub(crate) struct BundledRootsConnector;

impl HttpConnector for BundledRootsConnector {
	fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
		#[cfg(not(target_os = "android"))]
		if system_certificates_enabled(options) {
			let mut options = options.clone();
			for root in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
				options = options
					.with_root_certificate(object_store::Certificate::from_der(root.as_ref())?);
			}
			return ReqwestConnector::default().connect(&options);
		}
		ReqwestConnector::default().connect(options)
	}
}

/// Whether `options` leaves the system trust store in use, read with the
/// boolean spellings `object_store` accepts for `no_system_certificates`.
#[cfg(not(target_os = "android"))]
fn system_certificates_enabled(options: &ClientOptions) -> bool {
	match options.get_config_value(&object_store::ClientConfigKey::NoSystemCertificates) {
		None => true,
		Some(value) => {
			matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no" | "n")
		}
	}
}

#[cfg(all(test, not(target_os = "android")))]
mod tests {
	use object_store::{ClientConfigKey, ClientOptions};

	use super::*;

	#[test]
	fn system_certificates_are_enabled_by_default() {
		assert!(system_certificates_enabled(&ClientOptions::new()));
	}

	#[test]
	fn no_system_certificates_disables_the_bundled_roots() {
		for value in ["true", "1", "on", "yes", "y", "TRUE"] {
			let options =
				ClientOptions::new().with_config(ClientConfigKey::NoSystemCertificates, value);
			assert!(!system_certificates_enabled(&options), "{value}");
		}
		let options = ClientOptions::new().with_no_system_certificates(true);
		assert!(!system_certificates_enabled(&options));
	}

	#[test]
	fn a_false_no_system_certificates_keeps_the_bundled_roots() {
		for value in ["false", "0", "off", "no", "n", "False"] {
			let options =
				ClientOptions::new().with_config(ClientConfigKey::NoSystemCertificates, value);
			assert!(system_certificates_enabled(&options), "{value}");
		}
		let options = ClientOptions::new().with_no_system_certificates(false);
		assert!(system_certificates_enabled(&options));
	}

	#[test]
	fn an_unparseable_no_system_certificates_is_left_to_object_store() {
		let options =
			ClientOptions::new().with_config(ClientConfigKey::NoSystemCertificates, "maybe");
		assert!(!system_certificates_enabled(&options));
		assert!(BundledRootsConnector.connect(&options).is_err());
	}

	#[test]
	fn builds_a_client_with_the_bundled_roots() {
		assert!(!webpki_root_certs::TLS_SERVER_ROOT_CERTS.is_empty());
		BundledRootsConnector.connect(&ClientOptions::new()).expect("client builds");
	}

	/// Printed by [`empty_system_store_child`] once its assertions hold.
	#[cfg(target_os = "linux")]
	const EMPTY_STORE_MARKER: &str = "empty-system-store-checked";

	/// On Linux the platform verifier reads the system store from
	/// `SSL_CERT_FILE` when it is set, so pointing it at an empty file
	/// reproduces an image without a CA bundle. The check runs in a child
	/// process so no other test sees the empty store.
	#[cfg(target_os = "linux")]
	#[test]
	fn builds_a_client_when_the_system_store_is_empty() {
		let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
			.args([
				"--exact",
				"obs::connector::tests::empty_system_store_child",
				"--ignored",
				"--nocapture",
				"--test-threads=1",
			])
			.env("SSL_CERT_FILE", "/dev/null")
			.env_remove("SSL_CERT_DIR")
			.output()
			.expect("child test runs");
		let stdout = String::from_utf8_lossy(&output.stdout);
		assert!(
			output.status.success() && stdout.contains(EMPTY_STORE_MARKER),
			"child failed: {}\n{stdout}\n{}",
			output.status,
			String::from_utf8_lossy(&output.stderr),
		);
	}

	#[cfg(target_os = "linux")]
	#[test]
	#[ignore = "run in a child process by builds_a_client_when_the_system_store_is_empty"]
	fn empty_system_store_child() {
		if std::env::var_os("SSL_CERT_FILE").as_deref() != Some("/dev/null".as_ref()) {
			return;
		}
		// The store is empty: the default connector cannot build a client.
		assert!(ReqwestConnector::default().connect(&ClientOptions::new()).is_err());
		BundledRootsConnector.connect(&ClientOptions::new()).expect("client builds");
		println!("{EMPTY_STORE_MARKER}");
	}
}
