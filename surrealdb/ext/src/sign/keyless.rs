//! Keyless Sigstore signing from CI, through `sigstore-sign`.
//!
//! 1. The OIDC identity token (audience `sigstore`) comes from the CI environment
//!    ([`IdentityToken::detect_ambient`]: GitHub Actions, GitLab's `SIGSTORE_ID_TOKEN`, and the
//!    other providers `ambient-id` knows).
//! 2. `sigstore-sign` generates an ephemeral P-256 key, gets a short-lived certificate for it from
//!    Fulcio, signs `MANIFEST`, timestamps the signature at the TSA and records it in Rekor v2. The
//!    signed digest is `sha256(MANIFEST)`, the archive's `DIGEST`.
//!
//! Sigstore's published signing config lists only Rekor v1 logs, so the v2
//! log and the TSA are named here, each overridable by a flag. Rekor v2
//! records no time, so the TSA's timestamp is the signing time a verifier
//! checks the certificate against.
//!
//! The token and the key never leave the library, and no error message or
//! log line here includes them.

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use serde_json::Value;
use sigstore_oidc::IdentityToken;
use sigstore_rekor::RekorApiVersion;
use sigstore_sign::{SigningContext, SigningServices};

use super::archive::Manifest;

/// Sigstore public-good certificate authority.
pub const DEFAULT_FULCIO_URL: &str = "https://fulcio.sigstore.dev";
/// Sigstore public-good Rekor v2 log shard.
pub const DEFAULT_REKOR_URL: &str = "https://log2025-1.rekor.sigstore.dev";
/// Sigstore public-good RFC 3161 timestamp authority.
pub const DEFAULT_TSA_URL: &str = "https://timestamp.sigstore.dev/api/v1/timestamp";

/// Service endpoints for keyless signing.
pub struct Endpoints {
	pub fulcio: String,
	pub rekor: String,
	pub tsa: String,
}

/// `url` without trailing slashes, so two spellings of one service compare
/// equal.
fn normalise(url: &str) -> &str {
	url.trim_end_matches('/')
}

impl Endpoints {
	/// Whether every endpoint is Sigstore's public-good one, whose trusted
	/// root the public TUF repository serves.
	pub fn is_public_good(&self) -> bool {
		normalise(&self.fulcio) == DEFAULT_FULCIO_URL
			&& normalise(&self.rekor) == DEFAULT_REKOR_URL
			&& normalise(&self.tsa) == DEFAULT_TSA_URL
	}
}

/// A Sigstore bundle v0.3 over `manifest`, as JSON.
///
/// The identity token's claims are compared with `expected` before Fulcio is
/// asked for a certificate, so a job that is not the package's signer
/// leaves no entry in the transparency log.
pub async fn sign(
	manifest: &Manifest,
	endpoints: &Endpoints,
	expected: &surrealism_runtime::config::PackageSigner,
) -> Result<Vec<u8>> {
	let token = IdentityToken::detect_ambient()
		.await
		.context("Failed to read the CI identity token")?
		.context(
			"No CI identity token: run in GitHub Actions with `permissions: id-token: write`, \
			 or set SIGSTORE_ID_TOKEN (audience `sigstore`)",
		)?;
	super::signer::ensure_token_matches(&token_claims(token.expose_secret())?, expected)?;
	let services = SigningServices::new(normalise(&endpoints.fulcio), normalise(&endpoints.rekor))
		.with_rekor(normalise(&endpoints.rekor), RekorApiVersion::V2)
		.with_tsa_url(normalise(&endpoints.tsa));
	let bundle = SigningContext::new(services)
		.signer(token)
		.sign(manifest.as_bytes())
		.await
		.context("Sigstore signing failed")?;
	Ok(bundle.to_json().context("Failed to write the Sigstore bundle")?.into_bytes())
}

/// The identity token's payload, unverified: Fulcio verifies the token.
fn token_claims(token: &str) -> Result<Value> {
	let payload = token.split('.').nth(1).context("The identity token is not a JWT")?;
	let payload = B64URL
		.decode(payload.trim_end_matches('='))
		.context("The identity token payload is not base64url")?;
	serde_json::from_slice(&payload).context("The identity token payload is not JSON")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_token_payload_is_read_without_its_signature() {
		let payload = B64URL.encode(br#"{"iss":"https://gitlab.com","project_id":"1"}"#);
		let claims = token_claims(&format!("e30.{payload}.sig")).unwrap();
		assert_eq!(claims["iss"], "https://gitlab.com");
		assert!(token_claims("not-a-jwt").is_err());
	}

	#[test]
	fn the_public_good_endpoints_are_recognised_with_a_trailing_slash() {
		let endpoints = |suffix: &str| Endpoints {
			fulcio: format!("{DEFAULT_FULCIO_URL}{suffix}"),
			rekor: format!("{DEFAULT_REKOR_URL}{suffix}"),
			tsa: format!("{DEFAULT_TSA_URL}{suffix}"),
		};
		assert!(endpoints("").is_public_good());
		assert!(endpoints("/").is_public_good());
		let mut private = endpoints("");
		private.rekor = "https://rekor.example.com".into();
		assert!(!private.is_public_good());
	}

	/// Signs against public-good with a real CI token; needs network access,
	/// `SIGSTORE_ID_TOKEN` or a GitHub Actions job, and the job's own signer in
	/// `SIGNER_ISSUER` and `SIGNER_IDENTITY` (the workflow URI without `@ref`).
	#[tokio::test]
	#[ignore = "needs a CI identity token and network access"]
	async fn keyless_signing_against_public_good() {
		let manifest = Manifest::from_entries([(
			"surrealism/mod.wasm".to_string(),
			surrealism_runtime::manifest::Digest::of(b"\0asm"),
		)])
		.unwrap();
		let bundle = sign(
			&manifest,
			&Endpoints {
				fulcio: DEFAULT_FULCIO_URL.into(),
				rekor: DEFAULT_REKOR_URL.into(),
				tsa: DEFAULT_TSA_URL.into(),
			},
			&surrealism_runtime::config::PackageSigner::Sigstore(
				surrealism_runtime::config::SigstoreSigner {
					issuer: std::env::var("SIGNER_ISSUER").expect("SIGNER_ISSUER"),
					identity: std::env::var("SIGNER_IDENTITY").expect("SIGNER_IDENTITY"),
					repository_id: None,
					owner_id: None,
				},
			),
		)
		.await
		.unwrap();
		assert_eq!(bundle.first(), Some(&b'{'));
	}
}
