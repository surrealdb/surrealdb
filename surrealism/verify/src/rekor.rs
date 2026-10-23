//! Rekor v2 transparency log entries: body, inclusion proof and checkpoint.
//!
//! An entry proves that the signing was published. The proof is offline: the
//! entry's canonicalized body hashes to a Merkle leaf, the inclusion proof
//! leads from that leaf to a tree root, and the log key signs a checkpoint
//! (a C2SP signed note) that commits to that root and tree size.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::keys::PublicKey;
use crate::{Error, ManifestDigest, decode_base64};

/// A Merkle tree over 2^64 leaves has a path of at most 64 hashes.
const MAX_PROOF_HASHES: usize = 64;

/// The most signature lines a checkpoint may carry: the log's own plus witness
/// cosignatures. The bound limits the signature checks a hostile checkpoint
/// can demand.
const MAX_NOTE_SIGNATURES: usize = 16;

/// The signature line prefix of a signed note: an em dash and a space.
const NOTE_SIGNATURE_PREFIX: &str = "\u{2014} ";

/// A `TransparencyLogEntry` from a bundle, in protobuf JSON form.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TlogEntry {
	kind_version: KindVersion,
	inclusion_proof: Option<InclusionProof>,
	canonicalized_body: String,
}

#[derive(Deserialize)]
struct KindVersion {
	kind: String,
	version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InclusionProof {
	/// Protobuf JSON leaves a zero out, so the first entry has none.
	#[serde(default)]
	log_index: String,
	root_hash: String,
	tree_size: String,
	#[serde(default)]
	hashes: Vec<String>,
	checkpoint: Checkpoint,
}

#[derive(Deserialize)]
struct Checkpoint {
	envelope: String,
}

/// The canonicalized body of a `hashedrekord` 0.0.2 entry.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Body {
	api_version: String,
	kind: String,
	spec: Spec,
}

#[derive(Deserialize)]
struct Spec {
	#[serde(rename = "hashedRekordV002")]
	hashed_rekord_v002: HashedRekord,
}

#[derive(Deserialize)]
struct HashedRekord {
	data: HashOutput,
	signature: Signature,
}

#[derive(Deserialize)]
struct HashOutput {
	algorithm: String,
	digest: String,
}

#[derive(Deserialize)]
struct Signature {
	content: String,
	verifier: Verifier,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Verifier {
	key_details: String,
	x509_certificate: Option<RawBytes>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBytes {
	raw_bytes: String,
}

fn log_err(message: impl Into<String>) -> Error {
	Error::Log(message.into())
}

/// Parses a protobuf JSON `int64` string of decimal digits; empty is zero.
fn number(field: &str, value: &str) -> Result<u64, Error> {
	if value.is_empty() {
		return Ok(0);
	}
	if !value.bytes().all(|b| b.is_ascii_digit()) {
		return Err(Error::Malformed(format!("{field} is not a number")));
	}
	value.parse().map_err(|_| Error::Malformed(format!("{field} is out of range")))
}

/// Verifies that `entry` records `digest`, `signature` and `certificate` and is
/// included in a tree whose checkpoint `log` signed. Returns the entry's log
/// index.
pub(crate) fn verify_entry(
	entry: &TlogEntry,
	log: &PublicKey,
	log_spki: &[u8],
	digest: &ManifestDigest,
	signature: &[u8],
	certificate: &[u8],
) -> Result<u64, Error> {
	if entry.kind_version.kind != "hashedrekord" || entry.kind_version.version != "0.0.2" {
		return Err(log_err(format!(
			"unsupported entry kind {} {}",
			entry.kind_version.kind, entry.kind_version.version
		)));
	}
	let body_bytes = decode_base64("canonicalizedBody", &entry.canonicalized_body)?;
	check_body(&body_bytes, digest, signature, certificate)?;

	let proof = entry.inclusion_proof.as_ref().ok_or_else(|| log_err("no inclusion proof"))?;
	let index = number("logIndex", &proof.log_index)?;
	let size = number("treeSize", &proof.tree_size)?;
	let root = decode_base64("rootHash", &proof.root_hash)?;
	if proof.hashes.len() > MAX_PROOF_HASHES {
		return Err(log_err("inclusion proof is too long"));
	}
	let hashes = proof
		.hashes
		.iter()
		.map(|h| {
			<[u8; 32]>::try_from(decode_base64("inclusion proof hash", h)?)
				.map_err(|_| Error::Malformed("inclusion proof hash is not 32 bytes".into()))
		})
		.collect::<Result<Vec<_>, _>>()?;

	let leaf = leaf_hash(&body_bytes);
	if root_from_inclusion(index, size, leaf, &hashes).as_ref().map(|r| r.as_slice())
		!= Some(root.as_slice())
	{
		return Err(log_err("inclusion proof does not lead to the root hash"));
	}

	let checkpoint = verify_checkpoint(&proof.checkpoint.envelope, log, log_spki)?;
	if checkpoint.size != size || checkpoint.root != root {
		return Err(log_err("checkpoint commits to a different tree"));
	}
	Ok(index)
}

/// Requires the entry body to record exactly this digest, signature and
/// certificate.
fn check_body(
	body: &[u8],
	digest: &ManifestDigest,
	signature: &[u8],
	certificate: &[u8],
) -> Result<(), Error> {
	let body: Body = serde_json::from_slice(body)
		.map_err(|e| Error::Malformed(format!("entry body is not a hashedrekord 0.0.2: {e}")))?;
	if body.api_version != "0.0.2" || body.kind != "hashedrekord" {
		return Err(log_err("entry body is not a hashedrekord 0.0.2"));
	}
	let rekord = body.spec.hashed_rekord_v002;
	if rekord.data.algorithm != "SHA2_256"
		|| decode_base64("entry digest", &rekord.data.digest)? != digest
	{
		return Err(log_err("entry records a different digest"));
	}
	if decode_base64("entry signature", &rekord.signature.content)? != signature {
		return Err(log_err("entry records a different signature"));
	}
	let recorded = rekord
		.signature
		.verifier
		.x509_certificate
		.ok_or_else(|| log_err("entry records no certificate"))?;
	if rekord.signature.verifier.key_details != "PKIX_ECDSA_P256_SHA_256"
		|| decode_base64("entry certificate", &recorded.raw_bytes)? != certificate
	{
		return Err(log_err("entry records a different certificate"));
	}
	Ok(())
}

/// RFC 6962 leaf hash.
fn leaf_hash(data: &[u8]) -> [u8; 32] {
	let mut h = Sha256::new();
	h.update([0x00]);
	h.update(data);
	h.finalize().into()
}

/// RFC 6962 interior node hash.
fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
	let mut h = Sha256::new();
	h.update([0x01]);
	h.update(left);
	h.update(right);
	h.finalize().into()
}

/// The root an inclusion proof leads to (RFC 9162, section 2.1.3.2), or `None`
/// if the proof does not fit a tree of `size` leaves.
fn root_from_inclusion(
	index: u64,
	size: u64,
	leaf: [u8; 32],
	proof: &[[u8; 32]],
) -> Option<[u8; 32]> {
	if index >= size {
		return None;
	}
	let (mut f_n, mut s_n) = (index, size - 1);
	let mut r = leaf;
	for p in proof {
		if s_n == 0 {
			return None;
		}
		if f_n & 1 == 1 || f_n == s_n {
			r = node_hash(p, &r);
			while f_n & 1 == 0 && f_n != 0 {
				f_n >>= 1;
				s_n >>= 1;
			}
		} else {
			r = node_hash(&r, p);
		}
		f_n >>= 1;
		s_n >>= 1;
	}
	(s_n == 0).then_some(r)
}

struct VerifiedCheckpoint {
	size: u64,
	root: Vec<u8>,
}

/// Parses a C2SP signed-note checkpoint and requires a signature line from the
/// log: named after the checkpoint's origin, carrying the log key's hint, and
/// valid under the log key. Other signature lines, such as witness
/// cosignatures, are ignored.
fn verify_checkpoint(
	envelope: &str,
	log: &PublicKey,
	log_spki: &[u8],
) -> Result<VerifiedCheckpoint, Error> {
	let split = envelope.find("\n\n").ok_or_else(|| log_err("checkpoint has no signatures"))?;
	// The signed text includes the newline that ends the last body line.
	let (text, signatures) = (&envelope[..=split], &envelope[split + 2..]);
	let mut lines = text.lines();
	let origin = lines.next().filter(|l| !l.is_empty());
	let size = lines.next();
	let root = lines.next();
	let (Some(origin), Some(size), Some(root)) = (origin, size, root) else {
		return Err(log_err("checkpoint is missing its origin, size or root hash"));
	};
	if size.is_empty() || (size.len() > 1 && size.starts_with('0')) {
		return Err(log_err("checkpoint size is not canonical"));
	}
	let size = number("checkpoint size", size)?;
	let root = decode_base64("checkpoint root hash", root)?;
	if root.len() != 32 {
		return Err(log_err("checkpoint root hash is not 32 bytes"));
	}

	if !signatures.ends_with('\n') {
		return Err(log_err("checkpoint signatures do not end with a newline"));
	}
	if signatures.lines().count() > MAX_NOTE_SIGNATURES {
		return Err(log_err("checkpoint has too many signature lines"));
	}
	let hint = key_hint(origin, log, log_spki);
	let signed = signatures.lines().any(|line| {
		let Some((name, sig)) =
			line.strip_prefix(NOTE_SIGNATURE_PREFIX).and_then(|l| l.rsplit_once(' '))
		else {
			return false;
		};
		let Ok(sig) = BASE64.decode(sig) else {
			return false;
		};
		name == origin
			&& sig.len() > 4
			&& sig[..4] == hint
			&& match log {
				PublicKey::Ed25519(_) => log.verify_ed25519(text.as_bytes(), &sig[4..]),
				PublicKey::P256(_) | PublicKey::P384(_) => {
					log.verify_ecdsa_message(text.as_bytes(), &sig[4..])
				}
			}
	});
	if !signed {
		return Err(log_err("checkpoint is not signed by the `log` key"));
	}
	Ok(VerifiedCheckpoint {
		size,
		root,
	})
}

/// The four-byte key hint of a note signature. For Ed25519 it is the C2SP
/// key ID `sha256(name || "\n" || 0x01 || key)`; for an ECDSA log it is the
/// Sigstore log ID `sha256(SubjectPublicKeyInfo)`.
fn key_hint(name: &str, log: &PublicKey, log_spki: &[u8]) -> [u8; 4] {
	let id: [u8; 32] = match log {
		PublicKey::Ed25519(key) => {
			let mut h = Sha256::new();
			h.update(name.as_bytes());
			h.update([b'\n', 0x01]);
			h.update(key.as_bytes());
			h.finalize().into()
		}
		PublicKey::P256(_) | PublicKey::P384(_) => Sha256::digest(log_spki).into(),
	};
	[id[0], id[1], id[2], id[3]]
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The RFC 6962 tree over `leaves`, with every node a leaf hash.
	fn tree_root(leaves: &[[u8; 32]]) -> [u8; 32] {
		match leaves.len() {
			1 => leaves[0],
			n => {
				let k = n.next_power_of_two() / 2;
				node_hash(&tree_root(&leaves[..k]), &tree_root(&leaves[k..]))
			}
		}
	}

	fn audit_path(index: usize, leaves: &[[u8; 32]]) -> Vec<[u8; 32]> {
		if leaves.len() <= 1 {
			return Vec::new();
		}
		let k = leaves.len().next_power_of_two() / 2;
		if index < k {
			let mut p = audit_path(index, &leaves[..k]);
			p.push(tree_root(&leaves[k..]));
			p
		} else {
			let mut p = audit_path(index - k, &leaves[k..]);
			p.push(tree_root(&leaves[..k]));
			p
		}
	}

	#[test]
	fn inclusion_proofs_for_every_leaf_of_small_trees() {
		for size in 1..=17usize {
			let leaves: Vec<[u8; 32]> = (0..size).map(|i| leaf_hash(&[i as u8])).collect();
			let root = tree_root(&leaves);
			for index in 0..size {
				let path = audit_path(index, &leaves);
				let got = root_from_inclusion(index as u64, size as u64, leaves[index], &path);
				assert_eq!(got, Some(root), "size {size} index {index}");
				// The same proof cannot claim another position. It does not bind
				// the tree size; the signed checkpoint does.
				if size > 1 {
					let other = (index + 1) % size;
					assert_ne!(
						root_from_inclusion(other as u64, size as u64, leaves[index], &path),
						Some(root)
					);
				}
			}
		}
	}

	#[test]
	fn inclusion_refuses_index_outside_tree() {
		assert_eq!(root_from_inclusion(3, 3, [0; 32], &[]), None);
	}
}
