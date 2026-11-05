//! A signature made by the installed `ssh-keygen` with the exact command
//! `surreal ext sign --key` runs, checked by [`verify`].
//!
//! The committed fixtures in `ssh.rs` were made once; this makes a fresh key
//! and signature on every run, so a change in either `ssh-keygen` or the
//! verifier that breaks the contract between them shows up here. It is skipped
//! when `ssh-keygen` is not on `PATH`.

#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use sha2::{Digest as _, Sha256};
use surrealism_verify::{Error, Signer, VerifiedSigner, verify};

const MANIFEST: &[u8] = b"sha256:4be1a6c0e3f7a2b55b6d6bb0f87fbd5d0cb15a3b8c0b5c1b4d8e0f8e6a2c7d11  surrealism/exports.toml\n\
sha256:77f0d7a9b6e1c2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7  surrealism/mod.wasm\n";

/// A directory under the system temporary directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
	fn new() -> Self {
		let path = std::env::temp_dir()
			.join(format!("surrealism-verify-crosscheck-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&path);
		std::fs::create_dir(&path).unwrap();
		Self(path)
	}

	fn path(&self) -> &Path {
		&self.0
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

fn have_ssh_keygen() -> bool {
	Command::new("ssh-keygen")
		.arg("-?")
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.status()
		.is_ok()
}

/// A new key of `kind` (`ed25519` or `ecdsa`) in `dir`; returns its public line.
fn keygen(dir: &Path, name: &str, kind: &str) -> String {
	let path = dir.join(name);
	let mut args = vec!["-q", "-t", kind, "-N", "", "-C", "crosscheck@test", "-f"];
	if kind == "ecdsa" {
		args.splice(3..3, ["-b", "256"]);
	}
	let status = Command::new("ssh-keygen").args(&args).arg(&path).status().unwrap();
	assert!(status.success(), "ssh-keygen -t {kind} failed");
	std::fs::read_to_string(path.with_extension("pub")).unwrap().trim().to_owned()
}

/// `ssh-keygen -Y sign -n surrealism -O hashalg=sha256 -f KEY` over `MANIFEST`
/// on stdin, as `surreal ext sign --key` runs it.
fn sign(dir: &Path, name: &str) -> Vec<u8> {
	let mut child = Command::new("ssh-keygen")
		.args(["-Y", "sign", "-n", "surrealism", "-O", "hashalg=sha256", "-f"])
		.arg(dir.join(name))
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.unwrap();
	std::io::Write::write_all(&mut child.stdin.take().unwrap(), MANIFEST).unwrap();
	let output = child.wait_with_output().unwrap();
	assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
	output.stdout
}

#[test]
fn a_fresh_ssh_keygen_signature_verifies() {
	if !have_ssh_keygen() {
		eprintln!("skipping: ssh-keygen is not on PATH");
		return;
	}
	let dir = TempDir::new();
	let digest: [u8; 32] = Sha256::digest(MANIFEST).into();
	let other = keygen(dir.path(), "other", "ed25519");

	for kind in ["ed25519", "ecdsa"] {
		let key = keygen(dir.path(), kind, kind);
		let signature = sign(dir.path(), kind);

		let verified = verify(&digest, &signature, &Signer::Key(key.clone())).unwrap();
		let VerifiedSigner::Key {
			key_type,
			..
		} = verified
		else {
			panic!("a key signer verifies as a key: {verified:?}");
		};
		assert_eq!(key_type, key.split(' ').next().unwrap());

		assert_eq!(
			verify(&[0u8; 32], &signature, &Signer::Key(key.clone())),
			Err(Error::BadSignature),
			"{kind}: another digest"
		);
		assert_eq!(
			verify(&digest, &signature, &Signer::Key(other.clone())),
			Err(Error::KeyMismatch),
			"{kind}: another key"
		);
	}
}
