//! The install and upgrade flows, split into a read-only preparation that
//! builds a plan and a commit that writes it.
//!
//! Preparation fetches the archive, checks it, verifies its signature and
//! builds the statement; it writes nothing. The caller shows the plan, asks
//! for approval, and only then calls [`commit`].

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::Value;
use tracing::instrument;

use super::archive::{ArchiveFacts, preview_archive, unpack_archive};
use super::capabilities::Capabilities;
use super::installed::{Installed, fetch_installed};
use super::online::{self, Check, OnlineChecks};
use super::signer::{AnchorLabel, Signer, SignerMatch, compare};
use super::silo::{DetailsSource, MAX_PACKAGE_BYTES, PackageDetails, SiloClient, VersionRequest};
use super::source::{BucketFile, DefinedSource, Source};
use super::statement::{self, Decision, validate_name};
use super::verify::{
	Claims, VerifiedSigner, claims, load_trust_root, prove_signer, verify_envelope,
};

/// Settings shared by `install` and `upgrade`.
#[derive(Clone, Debug, Default)]
pub struct Options {
	/// Silo's address: [`super::silo::endpoint`], which tests replace with a
	/// local registry.
	pub silo_endpoint: String,
	/// `--trust-root`: a `trusted_root.json` to verify a Sigstore signature
	/// against, instead of the root its envelope embeds.
	pub trust_root: Option<PathBuf>,
	/// Where Sigstore's public trust root is fetched for the online checks;
	/// `None` turns the online checks off.
	pub public_root: online::PublicRoot,
	/// What the statement grants.
	pub grant: Grant,
	/// `--unsigned`: install without verifying any signature, and write
	/// `UNSIGNED`. The only way to skip verification.
	pub unsigned: bool,
	/// `--allow-downgrade`: an upgrade may move to a lower version.
	pub allow_downgrade: bool,
	/// `--allow-package-change`: an upgrade may move to another organisation
	/// or package.
	pub allow_package_change: bool,
}

/// How the capabilities to grant are decided.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Grant {
	/// What an interactive person reviews and then approves or refuses: the
	/// whole request on install; on upgrade, the installed grant carried over
	/// plus what is genuinely new.
	#[default]
	Request,
	/// Nothing that would need approval: on install, nothing; on upgrade,
	/// only the installed grant still within the new request. A
	/// non-interactive run without `--with-capability`.
	Nothing,
	/// `--with-capability` tokens, added to [`Grant::Nothing`]'s grant and
	/// held within the request.
	With(Vec<String>),
}

/// Where the archive was read from, for the prompt.
#[derive(Clone, Debug)]
pub enum Origin {
	Silo {
		/// What Silo's package API says about the release; `None` when the
		/// endpoint has no package API and only the archive was read.
		details: Option<Box<PackageDetails>>,
		/// Silo's release status: yanked, and any advisories.
		warnings: Vec<String>,
	},
	Bucket(BucketFile),
}

/// A checked archive and the signer it proves.
#[derive(Clone, Debug)]
pub struct Inspected {
	pub facts: ArchiveFacts,
	pub origin: Origin,
	/// The verified signer; `None` when the archive carries no `SIGNATURE`.
	pub signer: Option<Signer>,
	pub anchor: AnchorLabel,
	/// For a Sigstore signer: what was checked, offline and online.
	pub checks: Option<SigstoreChecks>,
	/// `--unsigned`: no signature was verified, whatever the archive carries.
	pub unsigned: bool,
}

/// How a Sigstore signature was checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigstoreChecks {
	/// The `--trust-root` file the signature verified against offline;
	/// `None` for the trust root its envelope embeds.
	pub trust_root: Option<PathBuf>,
	pub online: OnlineChecks,
}

/// What an install writes once approved.
#[derive(Clone, Debug)]
pub struct InstallPlan {
	pub inspected: Inspected,
	pub decision: Decision,
	pub statement: String,
}

/// What an upgrade writes once approved.
#[derive(Clone, Debug)]
pub struct UpgradePlan {
	pub installed: Installed,
	/// What the installed archive requested, when it could be read again.
	/// Without it, no request counts as new: see [`Capabilities::carry_over`].
	pub previous: Option<ArchiveFacts>,
	/// Whether the installed package's coordinates were known, so that a
	/// downgrade or a package change was checked. When `false`, the caller
	/// must confirm explicitly.
	pub package_known: bool,
	pub inspected: Inspected,
	pub decision: Decision,
	pub signer_match: SignerMatch,
	pub statement: String,
}

impl UpgradePlan {
	/// The new definition records exactly what is installed.
	pub fn is_noop(&self) -> bool {
		self.installed.digest.as_deref() == Some(self.decision.digest.as_str())
			&& self.installed.source == self.decision.source
			&& self.installed.capabilities == self.decision.capabilities
			&& matches!(self.signer_match, SignerMatch::Same | SignerMatch::BothUnsigned)
	}
}

/// Fetches and checks the archive behind `source`; returns it with the source
/// the statement will record.
#[instrument(level = "debug", skip(db, opts))]
pub async fn inspect(
	db: &Surreal<Any>,
	source: &Source,
	opts: &Options,
) -> Result<(Inspected, DefinedSource)> {
	let (bytes, facts, origin, defined) = match source {
		Source::Silo {
			org,
			pkg,
			version,
		} => {
			let client = SiloClient::new(&opts.silo_endpoint)?;
			let request = match version {
				Some(v) => VersionRequest::Exact(v.clone()),
				None => VersionRequest::Latest,
			};
			let resolved = client.resolve(org, pkg, &request).await?;
			// The signature is verified here, so the archive is read even when
			// the package API describes the release.
			let warnings = resolved.warnings();
			let (bytes, facts, details) = match resolved.source {
				DetailsSource::Archive {
					bytes,
					facts,
				} => (bytes, Some(*facts), None),
				DetailsSource::Api => {
					let bytes = client.archive(org, pkg, &resolved.version).await?;
					(bytes, None, Some(Box::new(resolved.details)))
				}
			};
			let defined = DefinedSource::Silo {
				org: org.clone(),
				pkg: pkg.clone(),
				version: resolved.version,
			};
			let origin = Origin::Silo {
				details,
				warnings,
			};
			(bytes, facts, origin, defined)
		}
		Source::Bucket(file) => {
			let bytes = bytes::Bytes::from(read_bucket_file(db, file).await?);
			(bytes, None, Origin::Bucket(file.clone()), DefinedSource::Bucket(file.clone()))
		}
	};

	let facts = match facts {
		Some(facts) => facts,
		None => preview_archive(bytes.clone()).await.context("the archive failed its checks")?,
	};

	if let DefinedSource::Silo {
		org,
		pkg,
		version,
	} = &defined
	{
		if (&facts.organisation, &facts.name, &facts.version) != (org, pkg, version) {
			bail!(
				"Silo served {}/{} {} for silo::{org}::{pkg}<{version}>",
				facts.organisation,
				facts.name,
				facts.version
			);
		}
		if let Origin::Silo {
			details: Some(details),
			..
		} = &origin
			&& details.digest.as_str() != facts.digest.to_string()
		{
			bail!(
				"Silo lists digest {} for this release, but the archive it served has {}",
				details.digest,
				facts.digest
			);
		}
	}

	// Verification is skipped only by an explicit --unsigned, never by an
	// answer to a question.
	let (signer, anchor, checks) = match &facts.signature {
		_ if opts.unsigned => (None, AnchorLabel::Key, None),
		// SIGNATURE is outside the MANIFEST, so removing it leaves DIGEST
		// valid; a package that names a [signer] must still carry one.
		None => match &facts.declared_signer {
			Some(declared) => bail!(
				"package.toml names signer {} but the archive carries no SIGNATURE (it may have \
				 been removed). Pass --unsigned to install without verifying.",
				super::signer::describe_declared(declared)
			),
			None => bail!("this extension is not signed; pass --unsigned to install it"),
		},
		Some(sig) => {
			let (signer, anchor, checks) = verify_signature_of(&facts, sig, opts).await?;
			(Some(signer), anchor, checks)
		}
	};
	// Only now is the archive unpacked, with the server's own loader: nothing
	// from an archive whose signature does not hold reaches the disk.
	unpack_archive(bytes, facts.digest).await.context("the archive failed its checks")?;

	Ok((
		Inspected {
			facts,
			origin,
			signer,
			anchor,
			checks,
			unsigned: opts.unsigned,
		},
		defined,
	))
}

/// Proves who signed the archive, refusing a signer its `package.toml` does
/// not name, and runs the online checks of a Sigstore signer.
///
/// An SSH signature proves its own key. A Sigstore signature verifies
/// offline, against the trust root its envelope embeds or the `--trust-root`
/// that replaces it; the online checks then say whether those anchors are
/// Sigstore's public root and whether the public log holds the entry.
pub async fn verify_signature_of(
	facts: &ArchiveFacts,
	signature: &[u8],
	opts: &Options,
) -> Result<(Signer, AnchorLabel, Option<SigstoreChecks>)> {
	// `extension sign` signs only as the package's [signer], so a signed
	// package names one.
	let Some(declared) = &facts.declared_signer else {
		bail!(
			"the archive carries a SIGNATURE, but its package.toml names no [signer]; \
			 `surreal extension sign` never produces such an archive"
		);
	};
	let claimed = claims(signature)?;
	// The cheap refusal first: a signature whose claims do not name the
	// package's [signer] is refused before any verification or network call.
	crate::cli::module::sign::signer::check(&claimed, declared).context(
		"the archive is signed by someone other than the [signer] its package.toml names",
	)?;
	let (signer, checks) = match &claimed {
		Claims::Key {
			..
		} => (prove_signer(&facts.digest, signature, None)?.0, None),
		Claims::Sigstore {
			..
		} => {
			// Offline first: the claims are proved before the online checks.
			let (signer, verified) = match &opts.trust_root {
				Some(path) => {
					prove_signer(&facts.digest, signature, Some(&load_trust_root(path)?))?
				}
				None => verify_envelope(&facts.digest, signature)?,
			};
			let (
				Signer::Sigstore(s),
				VerifiedSigner::Sigstore {
					signed_at,
					..
				},
			) = (&signer, &verified)
			else {
				bail!("a Sigstore signature proved a key signer");
			};
			let anchors = online::Verified {
				ca: &s.ca,
				log: &s.log,
				tsa: &s.tsa,
				signed_at: *signed_at,
			};
			let online = online::check(&opts.public_root, anchors, signature).await;
			let checks = SigstoreChecks {
				trust_root: opts.trust_root.clone(),
				online,
			};
			(signer, Some(checks))
		}
	};
	let anchor = match &checks {
		None => AnchorLabel::Key,
		Some(c) => match c.online.root {
			Check::Passed => AnchorLabel::PublicGood,
			Check::Failed(_) => AnchorLabel::CustomCa,
			Check::NotChecked(_) => AnchorLabel::Unchecked,
		},
	};
	Ok((signer, anchor, checks))
}

/// `RETURN file::get(<file>)` over the connection, refused when the file is
/// larger than the server would accept ([`MAX_PACKAGE_BYTES`]): its size is
/// asked first with `file::head`, and checked again on what arrives.
async fn read_bucket_file(db: &Surreal<Any>, file: &BucketFile) -> Result<Vec<u8>> {
	let head: Value = db
		.query("RETURN file::head($file).size")
		.bind(("file", file.to_value()))
		.await
		.with_context(|| format!("failed to read {file}"))?
		.check()
		.with_context(|| format!("failed to read {file}"))?
		.take(0)
		.with_context(|| format!("failed to read {file}"))?;
	match head {
		Value::None | Value::Null => bail!("{file} does not exist"),
		Value::Number(n) => match n.to_int() {
			Some(size) if size >= 0 && size as u64 <= MAX_PACKAGE_BYTES => {}
			_ => bail!("{file} is larger than {MAX_PACKAGE_BYTES} bytes, which the server accepts"),
		},
		other => bail!("{file} has no size: file::head returned {}", other.kind()),
	}
	let value: Value = db
		.query("RETURN file::get($file)")
		.bind(("file", file.to_value()))
		.await
		.with_context(|| format!("failed to read {file}"))?
		.check()
		.with_context(|| format!("failed to read {file}"))?
		.take(0)
		.with_context(|| format!("failed to read {file}"))?;
	match value {
		Value::Bytes(b) if b.len() as u64 <= MAX_PACKAGE_BYTES => Ok(b.into_inner().to_vec()),
		Value::Bytes(_) => bail!("{file} is larger than {MAX_PACKAGE_BYTES} bytes"),
		Value::None | Value::Null => bail!("{file} does not exist"),
		other => bail!("{file} returned {} instead of bytes", other.kind()),
	}
}

/// Plans `install <source> --as <name>`.
#[instrument(level = "debug", skip(db, opts))]
pub async fn prepare_install(
	db: &Surreal<Any>,
	source: &Source,
	name: &str,
	opts: &Options,
) -> Result<InstallPlan> {
	validate_name(name)?;
	let (inspected, defined) = inspect(db, source, opts).await?;
	let requests = &inspected.facts.requests;
	let capabilities = match &opts.grant {
		Grant::Request => requests.clone(),
		Grant::Nothing => Capabilities::nothing_within(requests),
		Grant::With(tokens) => {
			Capabilities::nothing_within(requests).with_tokens(tokens, requests)?
		}
	};
	let decision = Decision {
		name: name.to_string(),
		source: defined,
		digest: inspected.facts.digest.to_string(),
		signer: inspected.signer.clone(),
		capabilities,
		permissions: None,
		comment: None,
	};
	let statement = statement::define(&decision)?;
	Ok(InstallPlan {
		inspected,
		decision,
		statement,
	})
}

/// Plans `upgrade <name> [<source>]`. Without a source, a Silo extension
/// moves to the newest release of the same package and a bucket extension
/// re-reads its own file.
#[instrument(level = "debug", skip(db, opts))]
pub async fn prepare_upgrade(
	db: &Surreal<Any>,
	name: &str,
	source: Option<&Source>,
	opts: &Options,
) -> Result<UpgradePlan> {
	validate_name(name)?;
	let installed = fetch_installed(db, name).await?;
	let source = match source {
		Some(s) => s.clone(),
		None => match &installed.source {
			DefinedSource::Silo {
				org,
				pkg,
				..
			} => Source::Silo {
				org: org.clone(),
				pkg: pkg.clone(),
				version: None,
			},
			DefinedSource::Bucket(file) => Source::Bucket(file.clone()),
		},
	};
	let (inspected, defined) = inspect(db, &source, opts).await?;
	let previous = read_installed_archive(db, &installed, opts).await;
	let package_known =
		check_package_move(&installed, previous.as_ref(), &inspected.facts, &defined, opts)?;
	// Every choice starts from the installed grant, so what the install
	// withheld stays withheld unless --with-capability names it.
	let requests = &inspected.facts.requests;
	let capabilities = match &opts.grant {
		Grant::Request => Capabilities::carry_over(
			&installed.capabilities,
			previous.as_ref().map(|p| &p.requests),
			requests,
		),
		Grant::Nothing => Capabilities::carry_over(&installed.capabilities, None, requests),
		Grant::With(tokens) => Capabilities::carry_over(&installed.capabilities, None, requests)
			.with_tokens(tokens, requests)?,
	};
	let signer_match = compare(installed.signer.as_ref(), inspected.signer.as_ref());
	let decision = Decision {
		name: name.to_string(),
		source: defined,
		digest: inspected.facts.digest.to_string(),
		signer: inspected.signer.clone(),
		capabilities,
		permissions: Some(installed.permissions.clone()),
		comment: installed.comment.clone(),
	};
	let statement = statement::replace(&decision)?;
	Ok(UpgradePlan {
		package_known,
		previous,
		installed,
		inspected,
		decision,
		signer_match,
		statement,
	})
}

/// The installed archive, read again from the installed source, when it is
/// still there with the installed `DIGEST`. Only its package file is used:
/// its request and its package coordinates. `None` when it cannot be read or
/// has changed, which [`Capabilities::carry_over`] and [`check_package_move`]
/// treat conservatively.
async fn read_installed_archive(
	db: &Surreal<Any>,
	installed: &Installed,
	opts: &Options,
) -> Option<ArchiveFacts> {
	let bytes = match &installed.source {
		DefinedSource::Silo {
			org,
			pkg,
			version,
		} => SiloClient::new(&opts.silo_endpoint).ok()?.archive(org, pkg, version).await.ok()?,
		DefinedSource::Bucket(file) => read_bucket_file(db, file).await.ok()?.into(),
	};
	// The installed archive is only read for its package file and request:
	// nothing of it is unpacked.
	let facts = preview_archive(bytes).await.ok()?;
	(installed.digest.as_deref() == Some(facts.digest.to_string().as_str())).then_some(facts)
}

/// Refuses an upgrade that moves to a lower version, or to another
/// organisation or package, unless the matching flag allows it. Returns
/// `false` when the installed package cannot be determined, so nothing
/// could be checked.
fn check_package_move(
	installed: &Installed,
	previous: Option<&ArchiveFacts>,
	new: &ArchiveFacts,
	defined: &DefinedSource,
	opts: &Options,
) -> Result<bool> {
	let old = match (previous, &installed.source) {
		(Some(p), _) => (p.organisation.clone(), p.name.clone(), p.version.clone()),
		(
			None,
			DefinedSource::Silo {
				org,
				pkg,
				version,
			},
		) => (org.clone(), pkg.clone(), version.clone()),
		// Nothing says what the installed package was: the caller confirms.
		(None, DefinedSource::Bucket(_)) => return Ok(false),
	};
	let moving = format!(
		"from {}/{} {} ({}) to {}/{} {} ({})",
		old.0, old.1, old.2, installed.source, new.organisation, new.name, new.version, defined
	);
	if (old.0.as_str(), old.1.as_str()) != (new.organisation.as_str(), new.name.as_str())
		&& !opts.allow_package_change
	{
		bail!("this upgrade changes the package {moving}; pass --allow-package-change to allow it");
	}
	if new.version < old.2 && !opts.allow_downgrade {
		bail!("this upgrade is a downgrade {moving}; pass --allow-downgrade to allow it");
	}
	Ok(true)
}

/// Runs the statement. It is the only write an install or upgrade makes.
#[instrument(level = "debug", skip_all)]
pub async fn commit(db: &Surreal<Any>, statement: &str) -> Result<()> {
	db.query(statement)
		.await
		.context("the server refused the extension definition")?
		.check()
		.context("the server refused the extension definition")?;
	Ok(())
}

#[cfg(test)]
#[cfg(feature = "extension-sign")]
mod tests {
	use semver::Version;
	use sha2::{Digest as _, Sha256};
	use surrealism_runtime::config::{PackageSigner, SigstoreSigner as DeclaredSigstore};
	use surrealism_runtime::package::Digest;
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	use super::*;
	use crate::cli::extension::capabilities::Capabilities;
	use crate::cli::module::sign::crosscheck::{fixtures, json, private_trusted_root, read};

	const CASE: &str = "ed25519-log";

	/// The digest the private-Sigstore fixtures sign.
	fn digest() -> Digest {
		Digest(Sha256::digest(read(&fixtures().join("MANIFEST"))).into())
	}

	/// The fixture bundle sealed, as `extension sign` writes it, in an
	/// envelope with the private Sigstore's trust root.
	fn envelope() -> Vec<u8> {
		let bundle = read(&fixtures().join("private").join(CASE).join("bundle.sigstore.json"));
		crate::cli::module::sign::trusted_root::seal(
			&private_trusted_root(CASE),
			&digest().0,
			&bundle,
		)
		.expect("seal")
	}

	fn facts(declared: Option<PackageSigner>) -> ArchiveFacts {
		ArchiveFacts {
			digest: digest(),
			signature: None,
			organisation: "acme".into(),
			name: "color".into(),
			version: Version::new(1, 0, 0),
			repository: None,
			declared_signer: declared,
			requests: Capabilities::default(),
			exports: vec![],
		}
	}

	fn declared(identity: &str) -> PackageSigner {
		PackageSigner::Sigstore(DeclaredSigstore {
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: identity.into(),
			repository_id: Some("812345678".into()),
			owner_id: Some("98765432".into()),
		})
	}

	const WORKFLOW: &str = "https://github.com/acme/color/.github/workflows/release.yml";

	/// The facts of an archive whose package.toml names the fixture signer.
	fn signed() -> ArchiveFacts {
		facts(Some(declared(WORKFLOW)))
	}

	/// The private Sigstore's root as Sigstore's public one, with its log at
	/// `log_url` and its timestamp authority replaced by `tsa` when given.
	fn public_with(log_url: &str, tsa: Option<serde_json::Value>) -> Vec<u8> {
		let mut root: serde_json::Value =
			serde_json::from_slice(&private_trusted_root(CASE)).unwrap();
		for log in root["tlogs"].as_array_mut().unwrap() {
			if log["baseUrl"] == "https://log.surrealism.test" {
				log["baseUrl"] = log_url.into();
			}
		}
		if let Some(tsa) = tsa {
			root["timestampAuthorities"] = tsa;
		}
		serde_json::to_vec(&root).unwrap()
	}

	/// Serves the fixture bundle's log entry as the Rekor v2 log at `server`.
	async fn serve_entry(server: &MockServer) {
		let bundle = json(&fixtures().join("private").join(CASE).join("bundle.sigstore.json"));
		let entry = &bundle["verificationMaterial"]["tlogEntries"][0];
		let index: u64 = entry["logIndex"].as_str().unwrap().parse().unwrap();
		let body = base64::Engine::decode(
			&base64::engine::general_purpose::STANDARD,
			entry["canonicalizedBody"].as_str().unwrap(),
		)
		.unwrap();
		let mut tile = Vec::new();
		for i in 0..=(index % 256) {
			let e: &[u8] = if i == index % 256 {
				&body
			} else {
				b"other"
			};
			tile.extend_from_slice(&(e.len() as u16).to_be_bytes());
			tile.extend_from_slice(e);
		}
		// The log's current checkpoint: the one the fixture bundle was logged
		// under, signed by the private Sigstore's log key.
		let checkpoint =
			entry["inclusionProof"]["checkpoint"]["envelope"].as_str().unwrap().to_string();
		Mock::given(method("GET"))
			.and(path("/api/v2/checkpoint"))
			.respond_with(ResponseTemplate::new(200).set_body_string(checkpoint))
			.mount(server)
			.await;
		Mock::given(method("GET"))
			// The fixture log is small: its entry bundles are `000`…`999`.
			.and(path(format!("/api/v2/tile/entries/{:03}", index / 256)))
			.respond_with(ResponseTemplate::new(200).set_body_bytes(tile))
			.mount(server)
			.await;
	}

	/// Options whose public root is `root`, as TUF would deliver it.
	fn given(root: &[u8]) -> Options {
		let root =
			crate::cli::extension::verify::parse_trusted_root(std::str::from_utf8(root).unwrap())
				.unwrap();
		Options {
			public_root: online::PublicRoot::Given(root),
			..Options::default()
		}
	}

	#[tokio::test]
	async fn an_envelope_verifies_offline() {
		let (signer, anchor, checks) =
			verify_signature_of(&facts(Some(declared(WORKFLOW))), &envelope(), &Options::default())
				.await
				.expect("verifies from its own trust root");
		let Signer::Sigstore(s) = &signer else {
			panic!("a Sigstore signer");
		};
		let expected = json(&fixtures().join("private").join(CASE).join("signer.json"));
		assert_eq!(s.identity, expected["identity"].as_str().unwrap());
		assert_eq!(s.log, expected["log"].as_str().unwrap());
		assert!(!s.tsa.is_empty(), "the server requires tsa");
		let checks = checks.expect("Sigstore checks");
		assert_eq!(checks.trust_root, None);
		assert!(matches!(checks.online.root, online::Check::NotChecked(_)));
		assert!(matches!(checks.online.log, online::Check::NotChecked(_)));
		assert_eq!(anchor, AnchorLabel::Unchecked);
	}

	#[tokio::test]
	async fn a_custom_root_is_flagged_and_a_public_one_recognised() {
		// Sigstore's (staging) root does not hold the private Sigstore.
		let staging = read(&fixtures().join("conformance").join("trusted_root.json"));
		let opts = given(&staging);
		let (_, anchor, checks) =
			verify_signature_of(&signed(), &envelope(), &opts).await.expect("verifies");
		let checks = checks.unwrap();
		assert_eq!(
			checks.online.root,
			online::Check::Failed("NOT a known Sigstore root (custom CA)".into())
		);
		assert_eq!(anchor, AnchorLabel::CustomCa);
		let none = super::super::RootAllowance::default();
		let err = super::super::confirm_sigstore_checks(Some(&checks), none).unwrap_err();
		assert!(
			err.to_string().contains("--allow-custom-root"),
			"only --allow-custom-root accepts it: {err}"
		);
		let custom = super::super::RootAllowance {
			custom: true,
			..Default::default()
		};
		super::super::confirm_sigstore_checks(Some(&checks), custom).expect("allowed");

		// A root that holds the anchors reads as Sigstore's own.
		let opts = given(&private_trusted_root(CASE));
		let (_, anchor, checks) =
			verify_signature_of(&signed(), &envelope(), &opts).await.expect("verifies");
		assert_eq!(checks.unwrap().online.root, online::Check::Passed);
		assert_eq!(anchor, AnchorLabel::PublicGood);
	}

	#[tokio::test]
	async fn an_envelope_passes_the_root_and_the_log_online() {
		let server = MockServer::start().await;
		serve_entry(&server).await;
		let opts = given(&public_with(&server.uri(), None));
		let (_, anchor, checks) =
			verify_signature_of(&signed(), &envelope(), &opts).await.expect("verifies");
		let checks = checks.unwrap();
		assert_eq!(checks.online.root, online::Check::Passed);
		assert_eq!(checks.online.log, online::Check::Passed, "the envelope's bundle is looked up");
		assert_eq!(anchor, AnchorLabel::PublicGood);
	}

	#[tokio::test]
	async fn a_timestamp_authority_sigstore_does_not_run_is_a_custom_root() {
		// The CA and log are the public ones, but the TSA is not.
		let staging = json(&fixtures().join("conformance").join("trusted_root.json"));
		let root =
			public_with("https://log.example", Some(staging["timestampAuthorities"].clone()));
		let opts = given(&root);
		let (_, anchor, checks) =
			verify_signature_of(&signed(), &envelope(), &opts).await.expect("verifies");
		assert!(matches!(checks.unwrap().online.root, online::Check::Failed(_)));
		assert_eq!(anchor, AnchorLabel::CustomCa);
	}

	#[test]
	fn an_unchecked_root_needs_its_flag() {
		let checks = SigstoreChecks {
			trust_root: None,
			online: OnlineChecks {
				root: online::Check::NotChecked("Sigstore's root is unreachable".into()),
				log: online::Check::NotChecked("".into()),
			},
		};
		let none = super::super::RootAllowance::default();
		let err = super::super::confirm_sigstore_checks(Some(&checks), none).unwrap_err();
		assert!(err.to_string().contains("--allow-unchecked-root"), "{err}");
		for allow in [(false, true), (true, false)] {
			let allowance = super::super::RootAllowance {
				custom: allow.0,
				unchecked: allow.1,
				log_failure: false,
			};
			super::super::confirm_sigstore_checks(Some(&checks), allowance).expect("allowed");
		}
	}

	#[tokio::test]
	async fn a_signature_without_a_declared_signer_is_refused() {
		let err =
			verify_signature_of(&facts(None), &envelope(), &Options::default()).await.unwrap_err();
		assert!(format!("{err:#}").contains("names no [signer]"), "{err:#}");
	}

	#[tokio::test]
	async fn a_tampered_trust_root_is_refused() {
		let mut parsed: serde_json::Value = serde_json::from_slice(&envelope()).unwrap();
		let other = json(&fixtures().join("private").join("ecdsa-log").join("signer.json"));
		parsed["trustedRoot"]["tlogs"][0]["publicKey"]["rawBytes"] = other["log"].clone();
		let tampered = serde_json::to_vec(&parsed).unwrap();
		let err = verify_signature_of(&signed(), &tampered, &Options::default()).await.unwrap_err();
		assert!(
			format!("{err:#}").contains("does not verify against the trust root it carries"),
			"{err:#}"
		);
	}

	#[tokio::test]
	async fn a_signer_the_package_does_not_name_is_refused() {
		let other = declared("https://github.com/acme/other/.github/workflows/release.yml");
		let err = verify_signature_of(&facts(Some(other)), &envelope(), &Options::default())
			.await
			.unwrap_err();
		assert!(format!("{err:#}").contains("[signer] its package.toml names"), "{err:#}");
	}
}
