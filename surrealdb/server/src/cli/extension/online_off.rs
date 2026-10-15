//! The online checks of a Sigstore signature, in a build without the
//! `extension-sign` feature.
//!
//! Fetching Sigstore's public trust root over TUF and proving a Rekor entry
//! need the Sigstore client crates, which only `extension-sign` links. Here
//! both checks report as not checked, exactly as under `--offline`, so an
//! install still needs `--allow-unchecked-root` (or `--allow-custom-root`) to
//! accept a Sigstore signature. Nothing passes silently.

/// Sigstore's public-good TUF repository, the default of `--sigstore-root-url`.
pub fn default_tuf_url() -> &'static str {
	"https://tuf-repo-cdn.sigstore.dev"
}

/// The outcome of one check, for the prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Check {
	#[allow(dead_code)]
	Passed,
	/// The check ran and the answer was no.
	#[allow(dead_code)]
	Failed(String),
	/// The check could not run.
	NotChecked(String),
}

/// Where Sigstore's public trust root would come from.
#[derive(Clone, Debug, Default)]
pub enum PublicRoot {
	/// No online checks (`--offline`).
	#[default]
	Off,
	/// The TUF repository at this URL.
	Tuf(#[allow(dead_code)] String),
}

/// The anchors a signature verified under, and when it was signed.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub struct Verified<'a> {
	pub ca: &'a [String],
	pub log: &'a str,
	pub tsa: &'a [String],
	pub signed_at: i64,
}

/// The online checks of a Sigstore signature, for the prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnlineChecks {
	/// Whether the signer's anchors are Sigstore's public-good root.
	pub root: Check,
	/// Whether the public log holds the signature's entry.
	pub log: Check,
}

/// Both checks, as not checked: this build cannot run them.
pub async fn check(
	source: &PublicRoot,
	_verified: Verified<'_>,
	_signature: &[u8],
) -> OnlineChecks {
	let why = match source {
		PublicRoot::Off => "online checks are off (--offline)",
		PublicRoot::Tuf(_) => {
			"online checks are not built in (the `extension-sign` feature is off)"
		}
	};
	OnlineChecks {
		root: Check::NotChecked(why.into()),
		log: Check::NotChecked(why.into()),
	}
}
