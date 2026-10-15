//! Key signatures: an SSH signature (the `SSHSIG` format git also uses) made by
//! `ssh-keygen -Y sign`.
//!
//! The signed message is the `MANIFEST` bytes, under namespace `surrealism`,
//! with hash algorithm `sha256`. The `SSHSIG` `H(message)` is therefore
//! `sha256(MANIFEST)`, which is the extension's `DIGEST`, so a verifier needs
//! only the digest. Running `ssh-keygen` rather than parsing the key in process
//! means encrypted keys, `ssh-agent` and hardware-backed keys work the same way
//! they do for `git commit -S`.

use std::io::{ErrorKind, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;

/// The `SSHSIG` namespace. A signature made for a git commit or any other
/// namespace does not verify as an extension signature.
pub const NAMESPACE: &str = "surrealism";

/// The `SSHSIG` hash algorithm. It must be `sha256` so that `H(message)`
/// equals `DIGEST`.
pub const HASH_ALGORITHM: &str = "sha256";

const ARMOR_BEGIN: &str = "-----BEGIN SSH SIGNATURE-----";
const ARMOR_END: &str = "-----END SSH SIGNATURE-----";

/// Signs `manifest` with the private key at `key` and returns the armored
/// signature. The passphrase prompt, if any, goes to the terminal and never
/// through this process.
pub fn sign(key: &Path, manifest: &[u8]) -> Result<Vec<u8>> {
	let mut child = Command::new("ssh-keygen")
		.args(["-Y", "sign", "-n", NAMESPACE, "-O"])
		.arg(format!("hashalg={HASH_ALGORITHM}"))
		.arg("-f")
		.arg(key)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::inherit())
		.spawn()
		.map_err(|e| match e.kind() {
			ErrorKind::NotFound => anyhow::anyhow!(
				"`ssh-keygen` was not found on PATH. Key signing needs OpenSSH 8.9 or later"
			),
			_ => anyhow::Error::new(e).context("Failed to run ssh-keygen"),
		})?;
	{
		let mut stdin = child.stdin.take().context("ssh-keygen has no stdin")?;
		stdin.write_all(manifest).context("Failed to pass the MANIFEST to ssh-keygen")?;
	}
	let output = child.wait_with_output().context("Failed to wait for ssh-keygen")?;
	if !output.status.success() {
		bail!(
			"ssh-keygen failed to sign with {} ({}). Key signing needs OpenSSH 8.9 or later",
			key.display(),
			output.status
		);
	}
	let (namespace, hash) = sshsig_header(&output.stdout)?;
	ensure!(namespace == NAMESPACE, "ssh-keygen signed in namespace {namespace:?}");
	ensure!(
		hash == HASH_ALGORITHM,
		"ssh-keygen signed with hash algorithm {hash:?}, not {HASH_ALGORITHM}. Key signing needs \
		 OpenSSH 8.9 or later"
	);
	Ok(output.stdout)
}

/// The namespace and hash algorithm of an armored `SSHSIG` signature.
fn sshsig_header(armored: &[u8]) -> Result<(String, String)> {
	let text = std::str::from_utf8(armored).context("The SSH signature is not text")?;
	let body: String = text
		.lines()
		.skip_while(|l| l.trim() != ARMOR_BEGIN)
		.skip(1)
		.take_while(|l| l.trim() != ARMOR_END)
		.collect();
	ensure!(!body.is_empty(), "ssh-keygen did not produce an SSH signature");
	let blob = B64.decode(body.trim()).context("The SSH signature is not base64")?;
	let rest = blob.strip_prefix(b"SSHSIG").context("The SSH signature has no SSHSIG magic")?;
	let (version, rest) = rest.split_at_checked(4).context("Truncated SSH signature")?;
	ensure!(version == [0, 0, 0, 1], "Unsupported SSHSIG version");
	let (_public_key, rest) = ssh_string(rest)?;
	let (namespace, rest) = ssh_string(rest)?;
	let (_reserved, rest) = ssh_string(rest)?;
	let (hash, _) = ssh_string(rest)?;
	Ok((
		String::from_utf8_lossy(namespace).into_owned(),
		String::from_utf8_lossy(hash).into_owned(),
	))
}

/// Splits an SSH wire-format `string` (u32 big-endian length, then bytes).
fn ssh_string(input: &[u8]) -> Result<(&[u8], &[u8])> {
	let (len, rest) = input.split_at_checked(4).context("Truncated SSH signature")?;
	let len = u32::from_be_bytes([len[0], len[1], len[2], len[3]]) as usize;
	rest.split_at_checked(len).context("Truncated SSH signature")
}

#[cfg(test)]
pub(crate) mod tests {
	use std::path::PathBuf;

	use super::*;

	pub fn have_ssh_keygen() -> bool {
		Command::new("ssh-keygen").arg("-?").output().is_ok()
	}

	/// Generates an unencrypted ed25519 key in `dir` and returns its path.
	pub fn generate_key(dir: &Path) -> PathBuf {
		let key = dir.join("id_ed25519");
		let status = Command::new("ssh-keygen")
			.args(["-q", "-t", "ed25519", "-N", "", "-C", "release@acme", "-f"])
			.arg(&key)
			.status()
			.unwrap();
		assert!(status.success());
		key
	}

	/// Checks `signature` over `manifest` with
	/// `ssh-keygen -Y check-novalidate -n NAMESPACE -s SIG < MANIFEST`.
	pub fn verifies(signature: &[u8], manifest: &[u8], namespace: &str, dir: &Path) -> bool {
		let sig = dir.join("check.sig");
		std::fs::write(&sig, signature).unwrap();
		let mut child = Command::new("ssh-keygen")
			.args(["-Y", "check-novalidate", "-n", namespace, "-s"])
			.arg(&sig)
			.stdin(Stdio::piped())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.unwrap();
		child.stdin.take().unwrap().write_all(manifest).unwrap();
		child.wait().unwrap().success()
	}

	#[test]
	fn a_key_signature_verifies_only_in_its_namespace() {
		if !have_ssh_keygen() {
			eprintln!("skipping: ssh-keygen not on PATH");
			return;
		}
		let dir = tempfile::tempdir().unwrap();
		let key = generate_key(dir.path());
		let manifest = b"sha256:00  surrealism/mod.wasm\n";
		let sig = sign(&key, manifest).unwrap();
		assert_eq!(
			sshsig_header(&sig).unwrap(),
			(NAMESPACE.to_string(), HASH_ALGORITHM.to_string())
		);
		assert!(verifies(&sig, manifest, NAMESPACE, dir.path()));
		assert!(!verifies(&sig, manifest, "git", dir.path()));
		assert!(!verifies(&sig, b"sha256:01  surrealism/mod.wasm\n", NAMESPACE, dir.path()));
	}

	#[test]
	fn a_sha512_signature_is_recognised() {
		if !have_ssh_keygen() {
			eprintln!("skipping: ssh-keygen not on PATH");
			return;
		}
		let dir = tempfile::tempdir().unwrap();
		let key = generate_key(dir.path());
		let mut child = Command::new("ssh-keygen")
			.args(["-Y", "sign", "-n", NAMESPACE, "-f"])
			.arg(&key)
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::null())
			.spawn()
			.unwrap();
		child.stdin.take().unwrap().write_all(b"m").unwrap();
		let sig = child.wait_with_output().unwrap().stdout;
		assert_eq!(sshsig_header(&sig).unwrap().1, "sha512");
	}
}
