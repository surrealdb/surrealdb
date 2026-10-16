//! The best-effort online checks of a Sigstore signature.
//!
//! The offline check, `surrealism_verify::verify` against the anchors of the
//! trust root the signature envelope embeds (or `--trust-root`), proves that
//! the bundle is internally consistent and covers the digest. It cannot say
//! whose certificate authority and log those anchors are. These checks add
//! that, when the network allows:
//!
//! - [`public_root`] obtains Sigstore's public trust root through TUF, verifying the repository's
//!   signed metadata from the root that `sigstore-trust-root` embeds, and [`public_entry`] says
//!   whether the signature's anchors are one of its entries.
//! - [`log_entry`] reads the signature's entry from the public Rekor v2 log that entry names,
//!   compares it with the entry the bundle carries, verifies the log's checkpoints with the public
//!   log key, and proves the entry's inclusion (RFC 9162) under the checkpoint that signs its tree.
//!
//! A check that cannot run is reported as not checked; it never passes
//! silently.

use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use sigstore_sign::crypto::SigningScheme;
use sigstore_sign::types::{DerPublicKey, SignatureBytes};
use sigstore_trust_root::{SigstoreInstance, TufBootstrap, TufConfig};

use super::signer::same_certs;
use super::verify::{SigstoreAnchor, TrustRoot, parse_trusted_root};

/// Sigstore's public-good TUF repository.
pub fn default_tuf_url() -> &'static str {
	SigstoreInstance::PublicGood.tuf_url()
}

/// The longest one request of an online check may take: its answers are a
/// few KB to a few MB.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Whether the checks refuse plain HTTP. Tests serve their mocks over HTTP.
const HTTPS_ONLY: bool = !cfg!(test);

/// Entries per tile in a C2SP tlog-tiles log.
const TILE_WIDTH: u64 = 256;

/// The largest entry bundle read: 256 entries of a few KB each, with room.
const MAX_ENTRY_BUNDLE_BYTES: u64 = 4 * 1024 * 1024;

/// The largest checkpoint read: a few lines and signatures.
const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024;

/// The most signature lines read from a checkpoint.
const MAX_NOTE_SIGNATURES: usize = 64;

/// The outcome of one check, for the prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Check {
	Passed,
	/// The check ran and the answer was no.
	Failed(String),
	/// The check could not run: offline, a service error, or nothing to check.
	NotChecked(String),
}

/// Where Sigstore's public trust root comes from.
#[derive(Clone, Debug, Default)]
pub enum PublicRoot {
	/// No online checks (`--offline`).
	#[default]
	Off,
	/// The TUF repository at this URL, verified from the public-good TUF root
	/// that `sigstore-trust-root` embeds.
	Tuf(String),
	/// A root given directly. Tests use it: a TUF repository cannot be
	/// mocked without its signing keys.
	#[cfg(test)]
	Given(TrustRoot),
}

/// The hardened client of [`super::net`]. Tests serve their mocks over
/// loopback HTTP.
fn client() -> Result<Client> {
	super::net::client(TIMEOUT, !HTTPS_ONLY)
}

/// Sigstore's public trust root.
///
/// For [`PublicRoot::Tuf`], the TUF client walks root → timestamp → snapshot
/// → targets from the embedded public-good TUF root and checks the
/// `trusted_root.json` target against the hashes the signed metadata pins.
/// Nothing is cached on disk. `None` for [`PublicRoot::Off`].
pub async fn public_root(source: &PublicRoot) -> Result<Option<TrustRoot>> {
	let fetched = match source {
		PublicRoot::Off => return Ok(None),
		#[cfg(test)]
		PublicRoot::Given(root) => return Ok(Some(root.clone())),
		PublicRoot::Tuf(url) => {
			let config = TufConfig::custom(
				url.clone(),
				TufBootstrap::trusted(SigstoreInstance::PublicGood.tuf_root()),
			)
			.without_cache()
			.with_http_client(client()?);
			sigstore_trust_root::TrustedRoot::from_tuf(config)
				.await
				.map_err(|e| Unavailable(format!("TUF repository {url}: {e}")))?
		}
	};
	// The anchors are compared in the form the statement records, so the
	// root goes through the same parser as a --trust-root file.
	let json = serde_json::to_string(&fetched).context("failed to read the trust root")?;
	parse_trusted_root(&json).map(Some).context("Sigstore's trust root holds no usable anchors")
}

/// The anchors a signature verified under, and when it was signed.
#[derive(Clone, Copy, Debug)]
pub struct Verified<'a> {
	/// Base64 DER certificates of the certificate authority.
	pub ca: &'a [String],
	/// Base64 DER of the log key.
	pub log: &'a str,
	/// Base64 DER certificates of the timestamp authority.
	pub tsa: &'a [String],
	/// The signing time the timestamp authority attests, in seconds since the
	/// Unix epoch.
	pub signed_at: i64,
}

/// What [`public_entry`] found.
#[derive(Debug)]
pub enum PublicMatch<'a> {
	/// An entry with these anchors that was valid when the signature was made.
	Valid(&'a SigstoreAnchor),
	/// Entries with these anchors exist, but none was valid then.
	OutsideValidity,
	/// No entry holds these anchors: a custom root.
	None,
}

/// The entry of `public` that holds `verified`'s anchors: the same
/// certificate authority and log key, and timestamp authority certificates
/// that are all Sigstore's, since a self-made timestamp authority could
/// backdate a signature. The entry must have been valid at the signing time.
pub fn public_entry<'a>(verified: Verified<'_>, public: &'a TrustRoot) -> PublicMatch<'a> {
	let mut candidates = public
		.anchors
		.iter()
		.filter(|p| {
			same_certs(&p.ca, verified.ca)
				&& p.log == verified.log
				&& verified.tsa.iter().all(|t| p.tsa.contains(t))
		})
		.peekable();
	if candidates.peek().is_none() {
		return PublicMatch::None;
	}
	match candidates.find(|p| p.covers(verified.signed_at)) {
		Some(entry) => PublicMatch::Valid(entry),
		None => PublicMatch::OutsideValidity,
	}
}

/// The online checks of a Sigstore signature, for the prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnlineChecks {
	/// Whether the signer's anchors are Sigstore's public-good root.
	pub root: Check,
	/// Whether the public log holds the signature's entry.
	pub log: Check,
}

/// Runs both checks for a signature verified under `verified`'s anchors. The
/// log is looked up only under a public root, at the address that root gives
/// for it; a custom CA's log is not one this client can vouch for.
pub async fn check(source: &PublicRoot, verified: Verified<'_>, signature: &[u8]) -> OnlineChecks {
	let public = match public_root(source).await {
		Ok(Some(public)) => public,
		Ok(None) => {
			let off = || Check::NotChecked("online checks are off (--offline)".into());
			return OnlineChecks {
				root: off(),
				log: off(),
			};
		}
		Err(e) => {
			return OnlineChecks {
				root: Check::NotChecked(format!("{e:#}")),
				log: Check::NotChecked(
					"the public root, which gives the log's address, could not be read".into(),
				),
			};
		}
	};
	match public_entry(verified, &public) {
		PublicMatch::Valid(entry) => OnlineChecks {
			root: Check::Passed,
			log: check_log(entry, signature).await,
		},
		PublicMatch::OutsideValidity => OnlineChecks {
			root: Check::Failed(
				"signed outside the validity of the public root entry that holds these anchors"
					.into(),
			),
			log: Check::NotChecked("the root entry was not valid at the signing time".into()),
		},
		PublicMatch::None => OnlineChecks {
			root: Check::Failed("NOT a known Sigstore root (custom CA)".into()),
			log: Check::NotChecked("the log is not one of Sigstore's".into()),
		},
	}
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BundleJson {
	verification_material: MaterialJson,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MaterialJson {
	#[serde(default)]
	tlog_entries: Vec<EntryJson>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntryJson {
	log_index: String,
	canonicalized_body: String,
	inclusion_proof: ProofJson,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProofJson {
	tree_size: String,
	root_hash: String,
	#[serde(default)]
	hashes: Vec<String>,
	checkpoint: CheckpointJson,
}

#[derive(Deserialize)]
struct CheckpointJson {
	envelope: String,
}

/// The `n`th tile path element of the C2SP tlog-tiles spec: `x001/x234/067`.
fn tile_index(n: u64) -> String {
	let digits = format!("{n}");
	let padded = format!("{}{digits}", "0".repeat((3 - digits.len() % 3) % 3));
	let groups: Vec<&str> =
		padded.as_bytes().chunks(3).map(|g| std::str::from_utf8(g).unwrap_or("000")).collect();
	let last = groups.len() - 1;
	groups
		.iter()
		.enumerate()
		.map(|(i, g)| {
			if i == last {
				(*g).to_string()
			} else {
				format!("x{g}")
			}
		})
		.collect::<Vec<_>>()
		.join("/")
}

/// The entries of an entry bundle: each a big-endian `u16` length and the
/// entry's bytes.
fn bundle_entries(mut tile: &[u8]) -> Result<Vec<&[u8]>> {
	let mut out = Vec::new();
	while !tile.is_empty() {
		ensure!(tile.len() >= 2, "the entry bundle is truncated");
		let len = usize::from(u16::from_be_bytes([tile[0], tile[1]]));
		ensure!(tile.len() >= 2 + len, "the entry bundle is truncated");
		out.push(&tile[2..2 + len]);
		tile = &tile[2 + len..];
	}
	Ok(out)
}

/// `GET {log_url}/api/v2/{path}`, its body read with a cap. `None` on 404;
/// any failure to ask is [`Unavailable`].
async fn get_capped(log_url: &str, path: &str, max_bytes: u64) -> Result<Option<Vec<u8>>> {
	let url = format!("{}/api/v2/{path}", log_url.trim_end_matches('/'));
	let mut response =
		client()?.get(&url).send().await.map_err(|e| Unavailable(format!("GET {url}: {e}")))?;
	if response.status() == StatusCode::NOT_FOUND {
		return Ok(None);
	}
	if !response.status().is_success() {
		return Err(Unavailable(format!("GET {url}: HTTP status {}", response.status())).into());
	}
	let body = super::silo::read_capped(&mut response, max_bytes)
		.await
		.map_err(|e| Unavailable(format!("{e:#}")))?;
	Ok(Some(body.to_vec()))
}

/// The entry bundle holding `tile`: the full tile when the log has grown past
/// it; otherwise the partial tile as wide as the tree the bundle's proof was
/// made against.
async fn entry_bundle(log_url: &str, tile: u64, tree_size: u64) -> Result<Vec<u8>> {
	let path = format!("tile/entries/{}", tile_index(tile));
	if let Some(bytes) = get_capped(log_url, &path, MAX_ENTRY_BUNDLE_BYTES).await? {
		return Ok(bytes);
	}
	ensure!(
		(tile + 1) * TILE_WIDTH > tree_size && !tree_size.is_multiple_of(TILE_WIDTH),
		"the log does not serve the entry bundle {path}"
	);
	let partial = format!("{path}.p/{}", tree_size % TILE_WIDTH);
	get_capped(log_url, &partial, MAX_ENTRY_BUNDLE_BYTES)
		.await?
		.with_context(|| format!("the log does not serve the entry bundle {partial}"))
}

/// A transparency log's public key, as a public trust root gives it (DER
/// `SubjectPublicKeyInfo`), with the scheme its checkpoints are signed in.
struct LogKey {
	spki: Vec<u8>,
	scheme: SigningScheme,
	/// The raw Ed25519 key, for the C2SP key hint; `None` for ECDSA.
	ed25519: Option<[u8; 32]>,
}

impl LogKey {
	fn from_spki(spki: &[u8]) -> Result<Self> {
		// The fixed DER prefixes of the three key types Rekor uses.
		const ED25519: &[u8] =
			&[0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];
		const P256: &[u8] = &[
			0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06,
			0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
		];
		const P384: &[u8] = &[
			0x30, 0x76, 0x30, 0x10, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06,
			0x05, 0x2b, 0x81, 0x04, 0x00, 0x22, 0x03, 0x62, 0x00,
		];
		let key = |scheme, ed25519| Self {
			spki: spki.to_vec(),
			scheme,
			ed25519,
		};
		if let Some(raw) = spki.strip_prefix(ED25519) {
			let raw = raw.try_into().context("the Ed25519 log key is not 32 bytes")?;
			return Ok(key(SigningScheme::Ed25519, Some(raw)));
		}
		if spki.strip_prefix(P256).is_some_and(|p| p.len() == 65) {
			return Ok(key(SigningScheme::EcdsaP256Sha256, None));
		}
		if spki.strip_prefix(P384).is_some_and(|p| p.len() == 97) {
			return Ok(key(SigningScheme::EcdsaP384Sha384, None));
		}
		bail!("the log key is not an Ed25519, ECDSA P-256 or ECDSA P-384 key")
	}

	/// The four-byte key hint of a note signature. For Ed25519 it is the C2SP
	/// key ID `sha256(name || "\n" || 0x01 || key)`; for an ECDSA log it is the
	/// Sigstore log ID `sha256(SubjectPublicKeyInfo)`.
	fn hint(&self, name: &str) -> [u8; 4] {
		let id: [u8; 32] = match &self.ed25519 {
			Some(raw) => {
				let mut h = Sha256::new();
				h.update(name.as_bytes());
				h.update([b'\n', 0x01]);
				h.update(raw);
				h.finalize().into()
			}
			None => Sha256::digest(&self.spki).into(),
		};
		[id[0], id[1], id[2], id[3]]
	}

	fn verifies(&self, message: &[u8], signature: &[u8]) -> bool {
		sigstore_sign::crypto::verify_signature(
			&DerPublicKey::from_bytes(&self.spki),
			message,
			&SignatureBytes::from_bytes(signature),
			self.scheme,
		)
		.is_ok()
	}
}

/// What a checkpoint signed by the log commits to.
#[derive(Debug, PartialEq, Eq)]
struct Checkpoint {
	origin: String,
	size: u64,
	root: [u8; 32],
}

/// Parses a C2SP signed-note checkpoint and requires a signature line named
/// after its origin, carrying `key`'s hint, that `key` verifies over the note
/// text. Other lines, such as witness cosignatures, are ignored.
fn verify_checkpoint(note: &str, key: &LogKey) -> Result<Checkpoint> {
	let split = note.find("\n\n").context("the checkpoint has no signatures")?;
	// The signed text includes the newline that ends the last body line.
	let (text, signatures) = (&note[..=split], &note[split + 2..]);
	let mut lines = text.lines();
	let (Some(origin), Some(size), Some(root)) = (lines.next(), lines.next(), lines.next()) else {
		bail!("the checkpoint is missing its origin, size or root hash");
	};
	ensure!(!origin.is_empty(), "the checkpoint has no origin");
	ensure!(
		!size.is_empty() && (size == "0" || !size.starts_with('0')),
		"the checkpoint size is not canonical"
	);
	let size: u64 = size.parse().context("the checkpoint size is not a number")?;
	let root: [u8; 32] = STANDARD
		.decode(root)
		.ok()
		.and_then(|r| r.try_into().ok())
		.context("the checkpoint root hash is not 32 bytes of base64")?;
	ensure!(
		signatures.lines().count() <= MAX_NOTE_SIGNATURES,
		"the checkpoint has too many signature lines"
	);
	let hint = key.hint(origin);
	let signed = signatures.lines().any(|line| {
		let Some((name, sig)) = line.strip_prefix("\u{2014} ").and_then(|l| l.rsplit_once(' '))
		else {
			return false;
		};
		let Ok(sig) = STANDARD.decode(sig) else {
			return false;
		};
		name == origin
			&& sig.len() > 4
			&& sig[..4] == hint
			&& key.verifies(text.as_bytes(), &sig[4..])
	});
	ensure!(signed, "the checkpoint is not signed by the public log key");
	Ok(Checkpoint {
		origin: origin.to_string(),
		size,
		root,
	})
}

/// Reads the signature's log entry from the Rekor v2 log at `log_url` and
/// proves it is in the log the public root names:
///
/// 1. the log serves, at the entry's index, exactly the entry the bundle carries;
/// 2. the bundle's checkpoint is signed by the public log key (`log_key`, base64 DER), for the tree
///    size and root its inclusion proof names;
/// 3. the inclusion proof (RFC 9162) takes the served entry to that root;
/// 4. the log's current checkpoint is signed by the same key, for the same origin, and its tree is
///    no smaller.
///
/// A failure to ask the log is [`Unavailable`]; any other error means the
/// log does not prove the entry.
pub async fn log_entry(log_url: &str, log_key: &str, signature: &[u8]) -> Result<()> {
	let mut json: serde_json::Value =
		serde_json::from_slice(signature).context("SIGNATURE is not JSON")?;
	// An envelope holds the bundle under `bundle`; a bare bundle is itself.
	if json["mediaType"] == surrealism_verify::ENVELOPE_MEDIA_TYPE {
		json = json["bundle"].take();
	}
	let bundle: BundleJson =
		serde_json::from_value(json).context("SIGNATURE does not hold a Sigstore bundle")?;
	let entry = bundle
		.verification_material
		.tlog_entries
		.first()
		.context("the bundle carries no transparency log entry")?;
	let proof = &entry.inclusion_proof;
	let index: u64 = entry.log_index.parse().context("the bundle's logIndex is not a number")?;
	let tree_size: u64 =
		proof.tree_size.parse().context("the bundle's treeSize is not a number")?;
	let body = STANDARD
		.decode(&entry.canonicalized_body)
		.context("the bundle's canonicalizedBody is not base64")?;
	let spki = STANDARD.decode(log_key).context("the public log key is not base64")?;
	let key = LogKey::from_spki(&spki)?;

	// 1. The entry the log serves.
	let tile = entry_bundle(log_url, index / TILE_WIDTH, tree_size).await?;
	let entries = bundle_entries(&tile)?;
	let served = entries
		.get((index % TILE_WIDTH) as usize)
		.with_context(|| format!("the log has no entry {index}"))?;
	ensure!(
		*served == body.as_slice(),
		"the log's entry {index} is not the entry the signature carries"
	);

	// 2. The checkpoint that signs the proof's tree.
	let signed =
		verify_checkpoint(&proof.checkpoint.envelope, &key).context("the bundle's checkpoint")?;
	let root: [u8; 32] = STANDARD
		.decode(&proof.root_hash)
		.ok()
		.and_then(|r| r.try_into().ok())
		.context("the bundle's rootHash is not 32 bytes of base64")?;
	ensure!(
		signed.size == tree_size && signed.root == root,
		"the bundle's checkpoint does not sign the tree its inclusion proof names"
	);

	// 3. Inclusion of the served entry under that root.
	let hashes = proof
		.hashes
		.iter()
		.map(|h| {
			STANDARD
				.decode(h)
				.ok()
				.and_then(|h| <[u8; 32]>::try_from(h).ok())
				.map(Into::into)
				.context("an inclusion proof hash is not 32 bytes of base64")
		})
		.collect::<Result<Vec<_>>>()?;
	sigstore_merkle::verify_inclusion_proof(
		&sigstore_merkle::hash_leaf(served),
		index,
		tree_size,
		&hashes,
		&signed.root.into(),
	)
	.map_err(|e| anyhow::anyhow!("the inclusion proof does not hold: {e}"))?;

	// 4. The log as it stands now.
	let current = get_capped(log_url, "checkpoint", MAX_CHECKPOINT_BYTES)
		.await?
		.context("the log serves no checkpoint")?;
	let current = std::str::from_utf8(&current).context("the log's checkpoint is not UTF-8")?;
	let current = verify_checkpoint(current, &key).context("the log's current checkpoint")?;
	ensure!(current.origin == signed.origin, "the log's current checkpoint names another log");
	ensure!(
		current.size >= signed.size,
		"the log's current tree ({}) is smaller than the one the signature was logged in ({})",
		current.size,
		signed.size
	);
	Ok(())
}

/// [`log_entry`] as a [`Check`], for the log of the public root `entry`.
pub async fn check_log(entry: &SigstoreAnchor, signature: &[u8]) -> Check {
	let Some(url) = entry.log_url.as_deref() else {
		return Check::NotChecked("the trust root names no log URL".into());
	};
	if HTTPS_ONLY && !url.starts_with("https://") {
		return Check::NotChecked(format!("the log address {url} is not https"));
	}
	match log_entry(url, &entry.log, signature).await {
		Ok(()) => Check::Passed,
		Err(e) if is_offline(&e) => Check::NotChecked(format!("{e:#}")),
		Err(e) => Check::Failed(format!("{e:#}")),
	}
}

/// A service that could not be reached or answered with an error: it could
/// not be asked.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Unavailable(String);

/// Whether `e` means the service could not be asked, rather than that it
/// answered no.
fn is_offline(e: &anyhow::Error) -> bool {
	e.chain().any(|c| {
		c.downcast_ref::<reqwest::Error>().is_some() || c.downcast_ref::<Unavailable>().is_some()
	})
}

#[cfg(test)]
mod tests {
	use sigstore_sign::crypto::{KeyAlgorithm, KeyPair};
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	use super::*;

	const ORIGIN: &str = "log.surrealism.test - 1";

	/// A test log: an ECDSA P-256 key and its public key as DER SPKI
	/// (base64). The Ed25519 path is exercised by the private-Sigstore
	/// fixture's real checkpoint in the flow tests.
	struct TestLog {
		key: KeyPair,
		spki: String,
	}

	impl TestLog {
		fn new() -> Self {
			let key = KeyPair::generate(KeyAlgorithm::EcdsaP256).unwrap();
			let spki = key.public_key_der().unwrap().to_base64();
			Self {
				key,
				spki,
			}
		}

		/// A C2SP checkpoint for a tree of `size` with `root`, signed by this log.
		fn checkpoint(&self, size: u64, root: &[u8]) -> String {
			let text = format!("{ORIGIN}\n{size}\n{}\n", STANDARD.encode(root));
			let spki = STANDARD.decode(&self.spki).unwrap();
			let hint = LogKey::from_spki(&spki).unwrap().hint(ORIGIN);
			let mut sig = hint.to_vec();
			sig.extend_from_slice(self.key.sign(text.as_bytes()).unwrap().as_bytes());
			format!("{text}\n\u{2014} {ORIGIN} {}\n", STANDARD.encode(sig))
		}
	}

	/// A four-entry tree with `mine` at index 2: its root, and the inclusion
	/// proof for index 2.
	fn tree(mine: &[u8]) -> ([u8; 32], Vec<[u8; 32]>, Vec<Vec<u8>>) {
		use sigstore_merkle::{hash_children, hash_leaf};
		let entries: Vec<Vec<u8>> =
			vec![b"a".to_vec(), b"b".to_vec(), mine.to_vec(), b"d".to_vec()];
		let l: Vec<_> = entries.iter().map(|e| hash_leaf(e)).collect();
		let left = hash_children(&l[0], &l[1]);
		let root = hash_children(&left, &hash_children(&l[2], &l[3]));
		(*root.as_bytes(), vec![*l[3].as_bytes(), *left.as_bytes()], entries)
	}

	/// A bundle for entry 2 of `tree`, with `proof` and `checkpoint`.
	fn bundle(body: &[u8], root: &[u8; 32], proof: &[[u8; 32]], checkpoint: &str) -> Vec<u8> {
		serde_json::to_vec(&serde_json::json!({
			"verificationMaterial": { "tlogEntries": [{
				"logIndex": "2",
				"canonicalizedBody": STANDARD.encode(body),
				"inclusionProof": {
					"logIndex": "2",
					"treeSize": "4",
					"rootHash": STANDARD.encode(root),
					"hashes": proof.iter().map(|h| STANDARD.encode(h)).collect::<Vec<_>>(),
					"checkpoint": { "envelope": checkpoint }
				}
			}]}
		}))
		.unwrap()
	}

	fn tile(entries: &[Vec<u8>]) -> Vec<u8> {
		let mut out = Vec::new();
		for e in entries {
			out.extend_from_slice(&(e.len() as u16).to_be_bytes());
			out.extend_from_slice(e);
		}
		out
	}

	/// Serves `entries` as the partial tile of a four-entry log, and
	/// `checkpoint` as its current checkpoint.
	async fn serve(entries: &[Vec<u8>], checkpoint: &str) -> MockServer {
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/api/v2/tile/entries/000.p/4"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(tile(entries)))
			.mount(&server)
			.await;
		Mock::given(method("GET"))
			.and(path("/api/v2/checkpoint"))
			.respond_with(ResponseTemplate::new(200).set_body_string(checkpoint))
			.mount(&server)
			.await;
		server
	}

	fn anchor(url: &str, log: &TestLog) -> SigstoreAnchor {
		SigstoreAnchor {
			ca: vec![],
			log: log.spki.clone(),
			log_url: Some(url.into()),
			tsa: vec![],
			valid_from: None,
			valid_until: None,
		}
	}

	#[test]
	fn tile_paths_follow_the_spec() {
		assert_eq!(tile_index(0), "000");
		assert_eq!(tile_index(67), "067");
		assert_eq!(tile_index(1234), "x001/234");
		assert_eq!(tile_index(1_234_067), "x001/x234/067");
	}

	#[tokio::test]
	async fn a_good_proof_under_a_signed_checkpoint_passes() {
		let log = TestLog::new();
		let (root, proof, entries) = tree(b"mine");
		let checkpoint = log.checkpoint(4, &root);
		let server = serve(&entries, &checkpoint).await;
		let signature = bundle(b"mine", &root, &proof, &checkpoint);
		assert_eq!(check_log(&anchor(&server.uri(), &log), &signature).await, Check::Passed);

		// The log serves another entry at that index.
		let forged = bundle(b"other", &root, &proof, &checkpoint);
		assert!(matches!(check_log(&anchor(&server.uri(), &log), &forged).await, Check::Failed(_)));
	}

	#[tokio::test]
	async fn a_tampered_proof_fails() {
		let log = TestLog::new();
		let (root, mut proof, entries) = tree(b"mine");
		let checkpoint = log.checkpoint(4, &root);
		let server = serve(&entries, &checkpoint).await;
		proof[0][0] ^= 1;
		let signature = bundle(b"mine", &root, &proof, &checkpoint);
		let check = check_log(&anchor(&server.uri(), &log), &signature).await;
		assert!(
			matches!(&check, Check::Failed(why) if why.contains("inclusion proof")),
			"{check:?}"
		);
	}

	#[tokio::test]
	async fn a_checkpoint_under_another_key_fails() {
		let log = TestLog::new();
		let other = TestLog::new();
		let (root, proof, entries) = tree(b"mine");
		// The log signs with `other`, but the public root names `log`.
		let checkpoint = other.checkpoint(4, &root);
		let server = serve(&entries, &checkpoint).await;
		let signature = bundle(b"mine", &root, &proof, &checkpoint);
		let check = check_log(&anchor(&server.uri(), &log), &signature).await;
		assert!(
			matches!(&check, Check::Failed(why) if why.contains("not signed by the public log key")),
			"{check:?}"
		);
	}

	#[tokio::test]
	async fn an_unreachable_log_is_not_checked() {
		let log = TestLog::new();
		let (root, proof, _) = tree(b"mine");
		let signature = bundle(b"mine", &root, &proof, &log.checkpoint(4, &root));
		let check = check_log(&anchor("http://127.0.0.1:9", &log), &signature).await;
		assert!(matches!(check, Check::NotChecked(_)), "{check:?}");
		let mut no_url = anchor("", &log);
		no_url.log_url = None;
		assert!(matches!(check_log(&no_url, &signature).await, Check::NotChecked(_)));
	}

	#[tokio::test]
	async fn an_oversized_entry_bundle_is_not_read() {
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/api/v2/tile/entries/000"))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(vec![
				0u8;
				(MAX_ENTRY_BUNDLE_BYTES + 1)
					as usize
			]))
			.mount(&server)
			.await;
		let err = entry_bundle(&server.uri(), 0, 1000).await.unwrap_err();
		assert!(format!("{err:#}").contains("byte limit"), "{err:#}");
		assert!(is_offline(&err), "an oversized answer is a failure to ask, not a no");
	}

	/// The root `sigstore-trust-root` parses reads, through this client's
	/// parser, as the same anchors as the file read directly.
	#[test]
	fn a_library_root_converts_to_the_same_anchors() {
		let text = std::fs::read_to_string(
			crate::sign::crosscheck::fixtures().join("conformance/trusted_root.json"),
		)
		.unwrap();
		let library = sigstore_trust_root::TrustedRoot::from_json(&text).expect("library parses");
		let converted = parse_trusted_root(&serde_json::to_string(&library).unwrap()).unwrap();
		let direct = parse_trusted_root(&text).unwrap();
		assert_eq!(converted.anchors, direct.anchors);
	}
}
