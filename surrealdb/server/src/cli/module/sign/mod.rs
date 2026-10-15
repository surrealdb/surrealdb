//! `sign`: writes `surrealism/SIGNATURE` into a `.surli` archive.
//!
//! The signature covers `sha256(MANIFEST)`, never the archive bytes, so signing
//! re-packs the archive without changing `MANIFEST` or any other entry, and the
//! extension's `DIGEST` stays the same. An archive holds at most one signature;
//! signing again replaces it. Three sources are supported:
//!
//! - keyless (default): Sigstore with a CI identity token, see [`keyless`];
//! - `--key`: an SSH signature over `MANIFEST` (namespace `surrealism`, hash `sha256`), see
//!   [`ssh`];
//! - `--bundle`: a Sigstore bundle made elsewhere (for example by cosign) over the extracted
//!   `MANIFEST`, checked against this archive's digest.
//!
//! Whatever the source, the signature must claim the `[signer]` the archive's
//! own `package.toml` names, or nothing is written; see [`signer`]. Keyless
//! signing also checks the CI identity token before it asks Fulcio for a
//! certificate, so a job that is not the signer leaves no log entry.

mod archive;
mod bundle;
#[cfg(test)]
mod crosscheck;
mod keyless;
mod signer;
mod ssh;
mod trusted_root;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;

/// Arguments of the `sign` subcommand.
#[derive(Debug, Args)]
pub struct SignArgs {
	/// Sign with an SSH private key instead of Sigstore (runs `ssh-keygen -Y sign`)
	#[arg(long, value_name = "KEY", conflicts_with = "bundle")]
	key: Option<PathBuf>,

	/// Import a Sigstore bundle made over the archive's extracted MANIFEST
	#[arg(long, value_name = "FILE")]
	bundle: Option<PathBuf>,

	/// Fulcio certificate authority for keyless signing
	#[arg(long, value_name = "URL", default_value = keyless::DEFAULT_FULCIO_URL)]
	fulcio_url: String,

	/// Rekor v2 transparency log for keyless signing
	#[arg(long, value_name = "URL", default_value = keyless::DEFAULT_REKOR_URL)]
	rekor_url: String,

	/// RFC 3161 timestamp authority for keyless signing
	#[arg(long, value_name = "URL", default_value = keyless::DEFAULT_TSA_URL)]
	tsa_url: String,

	/// Sigstore trusted root (`trusted_root.json`) whose anchors verify the
	/// signature. Required with `--bundle` and with a private Sigstore;
	/// otherwise fetched through Sigstore's public TUF repository
	#[arg(long, value_name = "FILE", conflicts_with = "key")]
	trusted_root: Option<PathBuf>,

	/// Write the signed archive here instead of replacing the input
	#[arg(short = 'o', long, value_name = "FILE")]
	out: Option<PathBuf>,

	/// The `.surli` archive to sign
	#[arg(value_name = "FILE")]
	file: PathBuf,
}

/// The signature to store, by source.
enum Source<'a> {
	Keyless(keyless::Endpoints),
	Key(&'a Path),
	Bundle(&'a Path),
}

pub async fn init(args: SignArgs) -> Result<()> {
	let source = match (&args.key, &args.bundle) {
		(Some(key), _) => Source::Key(key),
		(None, Some(bundle)) => Source::Bundle(bundle),
		(None, None) => Source::Keyless(keyless::Endpoints {
			fulcio: args.fulcio_url.clone(),
			rekor: args.rekor_url.clone(),
			tsa: args.tsa_url.clone(),
		}),
	};
	// A Sigstore signature is written with the anchors that verify it.
	let root = match (&source, &args.trusted_root) {
		(Source::Key(_), _) => None,
		(_, Some(file)) => Some(trusted_root::RootSource::File(file)),
		(Source::Bundle(_), None) => anyhow::bail!(
			"--bundle needs --trusted-root: the trusted_root.json whose anchors verify the bundle"
		),
		(Source::Keyless(endpoints), None) if endpoints.is_public_good() => {
			Some(trusted_root::RootSource::PublicGood)
		}
		(Source::Keyless(_), None) => anyhow::bail!(
			"A private Sigstore (--fulcio-url, --rekor-url or --tsa-url) needs --trusted-root: \
			 the trusted_root.json holding its anchors"
		),
	};
	let out = args.out.as_deref().unwrap_or(&args.file);
	let digest = sign(&args.file, out, source, root).await?;
	println!("Signed {} ({digest})", out.display());
	Ok(())
}

/// Signs the archive at `input` and writes it to `output`. Returns the
/// manifest digest as `sha256:<hex>`.
///
/// With `root`, the signature is a Sigstore bundle, written as the envelope
/// that also holds `root` reduced to the anchors that verify it. The root is
/// loaded before anything is signed, so a missing root costs no log entry, and
/// nothing is written unless the envelope verifies offline.
async fn sign(
	input: &Path,
	output: &Path,
	source: Source<'_>,
	root: Option<trusted_root::RootSource<'_>>,
) -> Result<String> {
	let manifest = archive::read_manifest(input)?;
	archive::check_files(input, &manifest)?;
	let expected = signer::of_archive(input)?;
	let root = match root {
		Some(root) => Some(trusted_root::load(root).await?),
		None => None,
	};
	let signature = match source {
		Source::Keyless(endpoints) => keyless::sign(&manifest, &endpoints, &expected).await?,
		Source::Key(key) => ssh::sign(key, manifest.as_bytes())?,
		Source::Bundle(path) => {
			let bytes = std::fs::read(path)
				.with_context(|| format!("Failed to read {}", path.display()))?;
			bundle::check_import(&bytes, &manifest.digest().0)?
		}
	};
	let signature = match &root {
		Some(root) => trusted_root::seal(root, &manifest.digest().0, &signature)?,
		None => signature,
	};
	signer::ensure_matches(&signature, &expected)?;
	archive::write_signature(input, output, &manifest, &signature)?;
	Ok(manifest.digest().to_string())
}

#[cfg(test)]
mod tests {
	use surrealism_runtime::config::{PackageSigner, SshSigner};

	use super::archive::tests::{archive_with_manifest, dump};
	use super::archive::{MANIFEST_PATH, SIGNATURE_PATH, read_manifest};
	use super::*;

	/// The files of an archive whose package names `signer`.
	fn files(signer: Option<&PackageSigner>) -> Vec<(&'static str, Vec<u8>)> {
		let mut package = String::from(
			"[package]\norganisation = \"acme\"\nname = \"color\"\nversion = \"1.0.0\"\n",
		);
		match signer {
			Some(PackageSigner::Ssh(ssh)) => {
				package.push_str(&format!("[signer.ssh]\npublic_key = \"{}\"\n", ssh.public_key))
			}
			Some(PackageSigner::Sigstore(s)) => package.push_str(&format!(
				"[signer.sigstore]\nissuer = \"{}\"\nidentity = \"{}\"\n",
				s.issuer, s.identity
			)),
			None => {}
		}
		vec![
			("surrealism/mod.wasm", b"\0asm\x0d\0\x01\0".to_vec()),
			("surrealism/package.toml", package.into_bytes()),
			("surrealism/exports.toml", b"[functions]\n".to_vec()),
		]
	}

	fn borrowed<'a>(files: &'a [(&'static str, Vec<u8>)]) -> Vec<(&'static str, &'a [u8])> {
		files.iter().map(|(p, b)| (*p, b.as_slice())).collect()
	}

	fn archive(dir: &Path, signer: Option<&PackageSigner>) -> PathBuf {
		archive_with_manifest(dir, &borrowed(&files(signer)))
	}

	fn ssh_signer(key: &Path) -> PackageSigner {
		let public = std::fs::read_to_string(key.with_extension("pub")).unwrap();
		PackageSigner::Ssh(SshSigner {
			public_key: SshSigner::normalise(&public).unwrap(),
		})
	}

	fn github() -> PackageSigner {
		PackageSigner::Sigstore(signer::tests::github())
	}

	fn signature_of(path: &Path) -> Vec<u8> {
		let sigs: Vec<_> = dump(path).into_iter().filter(|(p, _, _)| p == SIGNATURE_PATH).collect();
		assert_eq!(sigs.len(), 1);
		sigs.into_iter().next().unwrap().2
	}

	#[tokio::test]
	async fn key_signing_end_to_end() {
		if !ssh::tests::have_ssh_keygen() {
			eprintln!("skipping: ssh-keygen not on PATH");
			return;
		}
		let dir = tempfile::tempdir().unwrap();
		let key = ssh::tests::generate_key(dir.path());
		let pkg = archive(dir.path(), Some(&ssh_signer(&key)));
		let before = read_manifest(&pkg).unwrap();

		let digest = sign(&pkg, &pkg, Source::Key(&key), None).await.unwrap();
		assert_eq!(digest, before.digest().to_string());

		let after = read_manifest(&pkg).unwrap();
		assert_eq!(after.as_bytes(), before.as_bytes());
		let sig = signature_of(&pkg);
		assert!(sig.starts_with(b"-----BEGIN SSH SIGNATURE-----"));
		assert!(ssh::tests::verifies(&sig, after.as_bytes(), ssh::NAMESPACE, dir.path()));

		// Signing again replaces the signature rather than adding one.
		sign(&pkg, &pkg, Source::Key(&key), None).await.unwrap();
		signature_of(&pkg);
	}

	#[tokio::test]
	async fn a_key_the_package_does_not_name_is_refused_and_nothing_is_written() {
		if !ssh::tests::have_ssh_keygen() {
			eprintln!("skipping: ssh-keygen not on PATH");
			return;
		}
		let dir = tempfile::tempdir().unwrap();
		let (named_dir, other_dir) = (dir.path().join("named"), dir.path().join("other"));
		std::fs::create_dir_all(&named_dir).unwrap();
		std::fs::create_dir_all(&other_dir).unwrap();
		let named = ssh::tests::generate_key(&named_dir);
		let other = ssh::tests::generate_key(&other_dir);
		let pkg = archive(dir.path(), Some(&ssh_signer(&named)));
		let original = std::fs::read(&pkg).unwrap();

		let err = sign(&pkg, &pkg, Source::Key(&other), None).await.unwrap_err();
		assert!(err.to_string().contains("[signer.ssh] names"), "{err}");
		assert_eq!(std::fs::read(&pkg).unwrap(), original);
	}

	#[tokio::test]
	async fn a_package_without_a_signer_is_not_signed() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = archive(dir.path(), None);
		let original = std::fs::read(&pkg).unwrap();
		let err = sign(&pkg, &pkg, Source::Key(Path::new("unused")), None).await.unwrap_err();
		assert!(err.to_string().contains("surreal extension config signer"), "{err}");
		assert_eq!(std::fs::read(&pkg).unwrap(), original);
	}

	#[tokio::test]
	async fn a_bundle_whose_certificate_cannot_be_read_is_refused() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = archive(dir.path(), Some(&github()));
		let digest = read_manifest(&pkg).unwrap().digest().0;
		let bundle_path = dir.path().join("bundle.json");
		std::fs::write(&bundle_path, bundle::tests::cosign_bundle(&digest, &digest)).unwrap();
		let out = dir.path().join("signed.surli");

		let err = sign(&pkg, &out, Source::Bundle(&bundle_path), None).await.unwrap_err();
		assert!(err.to_string().contains("claims to come from"), "{err}");
		assert!(!out.exists());
	}

	/// An imported bundle is written only with a trusted root whose anchors
	/// verify it. A cosign-shaped bundle with a dummy signature passes the
	/// import checks but verifies against no root, so nothing is written.
	#[tokio::test]
	async fn an_imported_bundle_that_the_trusted_root_does_not_verify_is_refused() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = archive(dir.path(), Some(&github()));
		let original = std::fs::read(&pkg).unwrap();
		let digest = read_manifest(&pkg).unwrap().digest().0;
		let bundle_path = dir.path().join("bundle.json");
		std::fs::write(&bundle_path, bundle::tests::cosign_bundle(&digest, &digest)).unwrap();
		let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
			.join("../../surrealism/verify/tests/fixtures/conformance/trusted_root.json");

		let err = sign(
			&pkg,
			&pkg,
			Source::Bundle(&bundle_path),
			Some(trusted_root::RootSource::File(&root)),
		)
		.await
		.unwrap_err();
		assert!(err.to_string().contains("does not verify against the trusted root"), "{err}");
		assert_eq!(std::fs::read(&pkg).unwrap(), original);
	}

	/// A MANIFEST edited to list another `mod.wasm` than the archive holds is
	/// never signed, and the archive is left as it was.
	#[tokio::test]
	async fn an_archive_whose_files_do_not_match_its_manifest_is_refused() {
		let dir = tempfile::tempdir().unwrap();
		let mut listed = files(Some(&github()));
		listed[0].1 = b"\0asm\x0d\0\x01\0 malicious".to_vec();
		let manifest = surrealism_runtime::manifest::Manifest::from_entries(
			listed
				.iter()
				.map(|(p, b)| (p.to_string(), surrealism_runtime::manifest::Digest::of(b))),
		)
		.unwrap();
		let held = files(Some(&github()));
		let mut entries = borrowed(&held);
		entries.push((MANIFEST_PATH, manifest.as_bytes()));
		let pkg = super::archive::tests::write_archive(&dir.path().join("pkg.surli"), &entries);
		let original = std::fs::read(&pkg).unwrap();
		let bundle_path = dir.path().join("bundle.json");
		let digest = manifest.digest().0;
		std::fs::write(&bundle_path, bundle::tests::cosign_bundle(&digest, &digest)).unwrap();

		let err = sign(&pkg, &pkg, Source::Bundle(&bundle_path), None).await.unwrap_err();
		assert!(err.to_string().contains("does not match"), "{err}");
		assert_eq!(std::fs::read(&pkg).unwrap(), original);
	}

	#[tokio::test]
	async fn a_bundle_for_another_manifest_is_refused_and_nothing_is_written() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = archive(dir.path(), Some(&github()));
		let original = std::fs::read(&pkg).unwrap();
		let bundle_path = dir.path().join("bundle.json");
		std::fs::write(&bundle_path, bundle::tests::cosign_bundle(&[0x42; 32], &[0x42; 32]))
			.unwrap();

		let err = sign(&pkg, &pkg, Source::Bundle(&bundle_path), None).await.unwrap_err();
		assert!(err.to_string().contains("does not sign this archive"), "{err}");
		assert_eq!(std::fs::read(&pkg).unwrap(), original);
		assert!(dump(&pkg).iter().any(|(p, _, _)| p == MANIFEST_PATH));
	}
}
