//! The trust root a Sigstore `SIGNATURE` carries in its envelope
//! ([`surrealism_verify::ENVELOPE_MEDIA_TYPE`]).
//!
//! The signature carries the anchors it verifies against, as an SSH signature
//! carries its public key, so it can be verified offline. No trusted root is
//! built into the CLI: it comes from `--trusted-root`, or for Sigstore's
//! public-good instance from its TUF repository when signing, through
//! `sigstore-trust-root`'s TUF client, which verifies the repository's
//! metadata from Sigstore's TUF root. It is reduced to
//! the entries that verify this bundle, and the envelope is verified offline
//! before anything is written ([`surrealism_verify::TrustedRoot::seal`]), so a
//! signed archive always verifies from its own contents.
//!
//! A root records which anchors the signer used; it does not make them
//! trusted. The server verifies only against a definition's `SIGNER`.

use std::path::Path;

use anyhow::{Context, Result, ensure};
use sigstore_trust_root::{TrustedRoot as SigstoreRoot, TufConfig};
use surrealism_verify::{MAX_TRUSTED_ROOT_LEN, TrustedRoot};

/// Where the trusted root comes from.
pub enum RootSource<'a> {
	/// A `trusted_root.json` file, for a private Sigstore or an imported
	/// bundle.
	File(&'a Path),
	/// Sigstore's public-good TUF repository.
	PublicGood,
}

/// The `trusted_root.json` bytes `source` names.
pub async fn load(source: RootSource<'_>) -> Result<Vec<u8>> {
	match source {
		RootSource::File(path) => {
			let bytes = std::fs::read(path)
				.with_context(|| format!("Failed to read {}", path.display()))?;
			ensure!(
				bytes.len() <= MAX_TRUSTED_ROOT_LEN,
				"{} is larger than {MAX_TRUSTED_ROOT_LEN} bytes",
				path.display()
			);
			Ok(bytes)
		}
		RootSource::PublicGood => {
			let root = SigstoreRoot::from_tuf(TufConfig::production())
				.await
				.context("Failed to fetch Sigstore's trusted root through TUF")?;
			serde_json::to_vec(&root).context("Failed to write Sigstore's trusted root")
		}
	}
}

/// The `SIGNATURE` for `bundle`, a Sigstore bundle over `digest`: the
/// envelope holding it and `root` reduced to the anchors that verify it.
/// Fails, before anything is written, when no anchors in `root` verify it.
pub fn seal(root: &[u8], digest: &[u8; 32], bundle: &[u8]) -> Result<Vec<u8>> {
	TrustedRoot::parse(root)?
		.seal(digest, bundle)
		.map_err(|e| anyhow::anyhow!("The signature does not verify against the trusted root: {e}"))
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Fetches Sigstore's public-good root through its TUF repository.
	#[tokio::test]
	#[ignore = "needs network access"]
	async fn the_public_good_root_is_fetched() {
		let bytes = load(RootSource::PublicGood).await.unwrap();
		TrustedRoot::parse(&bytes).unwrap();
	}
}
