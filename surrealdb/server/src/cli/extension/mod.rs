//! `surreal extension install` and `surreal extension upgrade`.
//!
//! Both collect the archive's details, check it as the server will, show the
//! person who signed it and what it asks for, and on approval write one
//! ordinary statement: `DEFINE EXTENSION` for an install, `DEFINE EXTENSION
//! OVERWRITE` for an upgrade. The server needs no install method of its own.

mod archive;
mod capabilities;
mod flow;
mod installed;
mod net;
#[cfg(feature = "extension-sign")]
mod online;
#[cfg(not(feature = "extension-sign"))]
#[path = "online_off.rs"]
mod online;
mod prompt;
mod signer;
mod silo;
mod source;
mod sql;
mod statement;
mod verify;

use std::io::IsTerminal as _;
use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use clap::Args;
use surrealdb::Surreal;
use surrealdb::engine::any::{self, Any, connect};
use tracing::instrument;

use self::capabilities::HighRisk;
use self::flow::{Grant, Options};
use self::signer::{SignerMatch, TrustSigner};
use self::source::Source;
use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};

/// The connection paragraph both commands' `--help` ends with.
macro_rules! connection_help {
	() => {
		"The command connects with the usual connection flags: -e/--endpoint, \
		 -u/--username and -p/--password (or -t/--token), --auth-level, --ns and \
		 --db, or the SURREAL_USER, SURREAL_PASS, SURREAL_TOKEN, SURREAL_AUTH_LEVEL, \
		 SURREAL_NAMESPACE and SURREAL_DATABASE environment variables. As that user it \
		 reads INFO FOR DB and, for a bucket source, the archive with file::get, and \
		 it runs DEFINE EXTENSION (DEFINE EXTENSION OVERWRITE for an upgrade). The \
		 user therefore needs permission to define extensions in that database, and \
		 read access to the bucket. Nothing else is written: a bucket archive must \
		 already be in the bucket."
	};
}

/// `--help` of `surreal extension install`.
pub const INSTALL_HELP: &str = concat!(
	"Install an extension into a database: inspect, approve, define.\n\n",
	"Reads the archive, checks it as the server will, verifies its signature, shows who \
	 signed it and what it asks for, and on approval writes one DEFINE EXTENSION \
	 statement with its DIGEST, SIGNER and the approved CAPABILITIES. An archive that is \
	 not signed installs only with --unsigned, which skips signature verification and \
	 records UNSIGNED; no question stands in for it.\n\n",
	connection_help!(),
	"\n\nEvery decision is a named flag. A run that gives a flag for each decision asks \
	 nothing; otherwise it asks in a terminal, and without one fails, naming the flags it \
	 needs. --trust-signer names the signer to accept (any, an SSH key fingerprint, or a \
	 Sigstore identity with an optional issuer). --with-capability grants one requested \
	 capability (net:<target>, functions:<pattern>, queries, scripting, write, \
	 no-strict-timeout, memory=<bytes>, timeout=<duration>); without it and without a \
	 terminal, nothing that needs approval is granted. --allow-custom-root, \
	 --allow-unchecked-root and --allow-log-failure accept a failed or skipped Sigstore \
	 check.\n\n",
	"  surreal extension install silo::acme::color --as color \\\n",
	"    -e wss://db.example.com -u root -p \"$SURREAL_PASS\" --ns app --db main \\\n",
	"    --trust-signer https://github.com/acme/color/.github/workflows/release.yml \\\n",
	"    --with-capability net:api.color.io:443 --with-capability functions:http::get",
);

/// `--help` of `surreal extension upgrade`.
pub const UPGRADE_HELP: &str = concat!(
	"Upgrade an installed extension: compare signer and capabilities, redefine.\n\n",
	"Reads the installed definition and the new archive, shows what changes, asks again \
	 when the signer differs, and on approval writes DEFINE EXTENSION OVERWRITE.\n\n",
	connection_help!(),
	"\n\nEvery decision is a named flag, as for install: --trust-signer, and \
	 --allow-signer-change as well when the signer differs from the installed one. Without \
	 --with-capability, a run without a terminal keeps the installed grant and grants \
	 nothing new.\n\n",
	"  surreal extension upgrade color -e wss://db.example.com -u root \\\n",
	"    -p \"$SURREAL_PASS\" --ns app --db main \\\n",
	"    --trust-signer https://github.com/acme/color/.github/workflows/release.yml\n\n",
	"Upgrading a bucket extension in place: when the new archive replaces the installed file \
	 under the same key, the installed archive can no longer be read, so the upgrade cannot \
	 check for a downgrade or a change of package. It then asks, and in automation needs both \
	 --allow-downgrade and --allow-package-change. Uploading each release under its own key \
	 (f\"modules:/color-1.1.0.surli\") and upgrading to it keeps both checks.",
);

/// Arguments shared by `install` and `upgrade`.
#[derive(Args)]
pub struct CommonArgs {
	/// The signer to accept without asking: `any`, an SSH key fingerprint
	/// (SHA256:…), or a Sigstore identity (the workflow URL) optionally
	/// followed by its issuer. Another signer is refused. Without it, the
	/// command asks in a terminal and fails without one.
	#[arg(long, value_name = "SIGNER")]
	trust_signer: Option<TrustSigner>,
	/// Grant one requested capability, without asking: net:<target>,
	/// functions:<pattern>, queries, scripting, write, no-strict-timeout,
	/// memory=<bytes> or timeout=<duration>. Repeat it for each. It cannot
	/// grant more than the archive requests.
	#[arg(long = "with-capability", value_name = "CAP")]
	with_capability: Vec<String>,
	/// A Sigstore trusted_root.json to verify a Sigstore signature against,
	/// instead of the trust root the signature carries. For a private
	/// Sigstore or an air-gapped install.
	#[arg(long, value_name = "FILE")]
	trust_root: Option<PathBuf>,
	/// The TUF repository Sigstore's public trust root is fetched from, to
	/// check whether a signature's trust root is Sigstore's own. Its metadata
	/// is verified from the public-good TUF root built into this binary, so
	/// a mirror serves the same root or none.
	#[arg(long, value_name = "URL", default_value = online::default_tuf_url())]
	sigstore_root_url: String,
	/// Skip the online checks of a Sigstore signature (Sigstore's public root
	/// and the public log). The offline verification still runs.
	#[arg(long)]
	offline: bool,
	/// Accept a Sigstore signature whose trust root is NOT Sigstore's public
	/// root, a custom CA, without asking. Implies `--allow-unchecked-root`.
	#[arg(long)]
	allow_custom_root: bool,
	/// Accept a Sigstore signature whose trust root could not be checked
	/// online (--offline, or Sigstore's root unreachable) without asking.
	#[arg(long)]
	allow_unchecked_root: bool,
	/// Accept a Sigstore signature that the public Rekor log does not
	/// confirm (✗) without asking.
	#[arg(long)]
	allow_log_failure: bool,
	/// Install without verifying the archive's signature, even when it has
	/// one, and record the extension as UNSIGNED. DIGEST still pins the
	/// code, and the capabilities are still shown and confirmed. The only way
	/// to install an unsigned archive.
	#[arg(long)]
	unsigned: bool,
	/// Show the review and ask the same questions, then print the statement
	/// instead of running it. Nothing is written.
	#[arg(long)]
	dry_run: bool,
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	auth: AuthArguments,
	#[command(flatten)]
	sel: DatabaseSelectionArguments,
}

/// Leaves out `auth`, which holds the password or token, and `conn`, whose
/// endpoint URL can carry user info, so that no `Debug` output of the
/// arguments (a log line, a panic, an instrumented span) can carry a
/// credential.
impl std::fmt::Debug for CommonArgs {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CommonArgs")
			.field("trust_signer", &self.trust_signer)
			.field("with_capability", &self.with_capability)
			.field("trust_root", &self.trust_root)
			.field("sigstore_root_url", &self.sigstore_root_url)
			.field("offline", &self.offline)
			.field("allow_custom_root", &self.allow_custom_root)
			.field("allow_unchecked_root", &self.allow_unchecked_root)
			.field("allow_log_failure", &self.allow_log_failure)
			.field("unsigned", &self.unsigned)
			.field("dry_run", &self.dry_run)
			.field("sel", &self.sel)
			.finish_non_exhaustive()
	}
}

/// `surreal extension install <source> --as <name>`
#[derive(Args, Debug)]
pub struct InstallArgs {
	/// silo::<org>::<pkg>[<x.y.z>], or f"<bucket>:/<path>.surli" for an
	/// archive already in a bucket of the database
	#[arg(value_name = "SOURCE")]
	source: String,
	/// The name to call it by: ext::<name>::<function>()
	#[arg(long = "as", value_name = "NAME")]
	name: String,
	#[command(flatten)]
	common: CommonArgs,
}

/// `surreal extension upgrade <name> [<source>]`
#[derive(Args, Debug)]
pub struct UpgradeArgs {
	/// The installed extension's name, as in ext::<name>
	#[arg(value_name = "NAME")]
	name: String,
	/// The new source. Defaults to the newest Silo release of the installed
	/// package, or the installed bucket file read again.
	#[arg(value_name = "SOURCE")]
	source: Option<String>,
	/// Accept a signer that differs from the installed one without asking.
	/// --trust-signer must still name it.
	#[arg(long)]
	allow_signer_change: bool,
	/// Allow moving to a lower version than the installed one.
	#[arg(long)]
	allow_downgrade: bool,
	/// Allow moving to another organisation or package.
	#[arg(long)]
	allow_package_change: bool,
	#[command(flatten)]
	common: CommonArgs,
}

fn options(common: &CommonArgs) -> Options {
	Options {
		silo_endpoint: silo::endpoint(),
		trust_root: common.trust_root.clone(),
		public_root: if common.offline {
			online::PublicRoot::Off
		} else {
			online::PublicRoot::Tuf(common.sigstore_root_url.clone())
		},
		unsigned: common.unsigned,
		grant: if !common.with_capability.is_empty() {
			Grant::With(common.with_capability.clone())
		} else if interactive() {
			Grant::Request
		} else {
			Grant::Nothing
		},
		allow_downgrade: false,
		allow_package_change: false,
	}
}

async fn open(common: &CommonArgs) -> Result<Surreal<Any>> {
	let endpoint = common.conn.endpoint.clone();
	let AuthArguments {
		username,
		password,
		token,
		auth_level,
	} = &common.auth;
	let is_local = any::__into_endpoint(&endpoint)?.parse_kind()?.is_local();
	let db = connect(endpoint).await?;
	if !is_local {
		if let (Some(user), Some(pass)) = (username, password) {
			let creds = CredentialsBuilder::default()
				.with_username(Some(user.clone()))
				.with_password(Some(pass.clone()))
				.with_namespace(Some(common.sel.namespace.clone()))
				.with_database(Some(common.sel.database.clone()));
			match auth_level {
				CredentialsLevel::Root => db.signin(creds.root()?).await?,
				CredentialsLevel::Namespace => db.signin(creds.namespace()?).await?,
				CredentialsLevel::Database => db.signin(creds.database()?).await?,
			};
		} else if let Some(t) = token {
			db.authenticate(t.clone()).await?;
		}
	}
	db.use_ns(common.sel.namespace.clone()).use_db(common.sel.database.clone()).await?;
	Ok(db)
}

/// Whether a person can be asked: standard input is a terminal.
fn interactive() -> bool {
	std::io::stdin().is_terminal()
}

/// One decision: answered yes by `flag` when it was given; otherwise asked
/// in a terminal, and without one an error that names `flag`.
fn confirm_by(question: &str, answered: bool, flag: &str) -> Result<()> {
	if answered {
		println!("{question} yes ({flag})");
		return Ok(());
	}
	if !interactive() {
		bail!("{question} needs an answer: run in a terminal, or pass {flag}");
	}
	let ok = dialoguer::Confirm::new()
		.with_prompt(question)
		.default(false)
		.interact()
		.context("failed to read the answer")?;
	if !ok {
		bail!("cancelled; nothing was written");
	}
	Ok(())
}

/// The signer decision: --trust-signer must name `proved`, or the person is
/// asked. An unsigned install (--unsigned) has no signer to decide.
fn confirm_signer(proved: Option<&signer::Signer>, trust: Option<&TrustSigner>) -> Result<()> {
	let Some(proved) = proved else {
		return Ok(());
	};
	match trust {
		Some(trust) if trust.accepts(proved) => {
			println!("Trust this signer? yes (--trust-signer)");
			Ok(())
		}
		Some(_) => bail!(
			"the archive is signed by {}, which is not the signer --trust-signer names; nothing \
			 was written",
			prompt::clean(&signer::describe_signer(proved))
		),
		None => confirm_by("Trust this signer?", false, "--trust-signer <signer|any>"),
	}
}

/// A new signer on upgrade, answered only by --allow-signer-change (and
/// --trust-signer must still name it).
fn confirm_signer_change(change: &SignerMatch, allow_signer_change: bool) -> Result<()> {
	if let SignerMatch::Different(_) = change {
		confirm_by(
			"The new version has a DIFFERENT signer. Trust the new signer?",
			allow_signer_change,
			"--allow-signer-change",
		)?;
	}
	Ok(())
}

/// Which failed or skipped Sigstore checks the flags accept.
#[derive(Clone, Copy, Debug, Default)]
struct RootAllowance {
	custom: bool,
	unchecked: bool,
	log_failure: bool,
}

impl CommonArgs {
	fn root_allowance(&self) -> RootAllowance {
		RootAllowance {
			custom: self.allow_custom_root,
			unchecked: self.allow_unchecked_root,
			log_failure: self.allow_log_failure,
		}
	}
}

/// The confirmations a Sigstore signer's checks need, each answered only by
/// its own flag: a root that could not be checked by `--allow-unchecked-root`
/// (or `--allow-custom-root`), a custom CA by `--allow-custom-root`, and a
/// log that does not confirm the signature by `--allow-log-failure`.
fn confirm_sigstore_checks(
	checks: Option<&flow::SigstoreChecks>,
	allow: RootAllowance,
) -> Result<()> {
	let Some(checks) = checks else {
		return Ok(());
	};
	if let online::Check::NotChecked(why) = &checks.online.root {
		println!(
			"\n  ⚠ Whether the signature's trust root is Sigstore's public root was NOT checked: {}",
			prompt::clean(why)
		);
		confirm_by(
			"Trust this signature's root without the online check?",
			allow.custom || allow.unchecked,
			"--allow-unchecked-root",
		)?;
	}
	if let online::Check::Failed(_) = &checks.online.root {
		println!("\n  ✗ The signature's trust root is NOT Sigstore's public root (a custom CA).");
		println!("    A custom CA can issue a certificate for any identity.");
		confirm_by(
			"Trust this custom certificate authority?",
			allow.custom,
			"--allow-custom-root",
		)?;
	}
	if let online::Check::Failed(why) = &checks.online.log {
		println!("\n  ✗ The public log does not confirm the signature: {}", prompt::clean(why));
		confirm_by(
			"Install although the public log does not confirm the signature?",
			allow.log_failure,
			"--allow-log-failure",
		)?;
	}
	Ok(())
}

/// The capability decision. With --with-capability the grant is the flags'
/// and is only shown; without, a person approves the request (high-risk
/// items twice), and a run without a terminal granted nothing new
/// (`Grant::Nothing`) and asks nothing.
fn confirm_capabilities(grant: &Grant, question: &str, risks: &[HighRisk]) -> Result<()> {
	match grant {
		Grant::Request => {
			confirm_by(question, false, "--with-capability")?;
			if !risks.is_empty() {
				println!("\n  ⚠ High-risk capabilities:");
				for r in risks {
					println!("      {}", prompt::clean(&r.describe()));
				}
				confirm_by("Grant these high-risk capabilities?", false, "--with-capability")?;
			}
		}
		Grant::With(_) => println!("Capabilities: as granted by --with-capability"),
		Grant::Nothing => println!(
			"Capabilities: nothing new granted (no terminal and no --with-capability); see the \
			 requests not granted above"
		),
	}
	Ok(())
}

#[instrument(level = "debug", skip_all, fields(source = %args.source, name = %args.name))]
pub async fn install(args: InstallArgs) -> Result<()> {
	let source = Source::parse(&args.source)?;
	let db = open(&args.common).await?;
	let opts = options(&args.common);
	let plan = flow::prepare_install(&db, &source, &args.name, &opts).await?;

	println!("{}", prompt::render_install(&plan));
	confirm_sigstore_checks(plan.inspected.checks.as_ref(), args.common.root_allowance())?;
	confirm_signer(plan.decision.signer.as_ref(), args.common.trust_signer.as_ref())?;
	confirm_capabilities(
		&opts.grant,
		"Install with these permissions?",
		&plan.decision.capabilities.high_risk(),
	)?;

	if args.common.dry_run {
		println!("{}", prompt::clean(&plan.statement));
		return Ok(());
	}
	flow::commit(&db, &plan.statement).await?;
	println!("{}\n\nInstalled ext::{}", prompt::clean(&plan.statement), plan.decision.name);
	Ok(())
}

#[instrument(level = "debug", skip_all, fields(name = %args.name))]
pub async fn upgrade(args: UpgradeArgs) -> Result<()> {
	let source = args.source.as_deref().map(Source::parse).transpose()?;
	let db = open(&args.common).await?;
	let opts = Options {
		allow_downgrade: args.allow_downgrade,
		allow_package_change: args.allow_package_change,
		..options(&args.common)
	};
	let plan = flow::prepare_upgrade(&db, &args.name, source.as_ref(), &opts).await?;

	if plan.is_noop() {
		println!("ext::{} is up to date", plan.decision.name);
		return Ok(());
	}
	println!("{}", prompt::render_upgrade(&plan));
	confirm_signer_change(&plan.signer_match, args.allow_signer_change)?;
	confirm_sigstore_checks(plan.inspected.checks.as_ref(), args.common.root_allowance())?;
	confirm_signer(plan.decision.signer.as_ref(), args.common.trust_signer.as_ref())?;
	if !plan.package_known {
		println!(
			"\n  ⚠ The installed archive cannot be read again, so this upgrade cannot be checked for a downgrade or a change of package."
		);
		confirm_by(
			"Upgrade without that check?",
			args.allow_downgrade && args.allow_package_change,
			"--allow-downgrade --allow-package-change",
		)?;
	}
	let installed_risks = plan.installed.capabilities.high_risk();
	let new_risks: Vec<HighRisk> = plan
		.decision
		.capabilities
		.high_risk()
		.into_iter()
		.filter(|r| !installed_risks.contains(r))
		.collect();
	confirm_capabilities(&opts.grant, "Upgrade with these permissions?", &new_risks)?;

	if args.common.dry_run {
		println!("{}", prompt::clean(&plan.statement));
		return Ok(());
	}
	flow::commit(&db, &plan.statement).await?;
	println!("{}\n\nUpgraded ext::{}", prompt::clean(&plan.statement), plan.decision.name);
	Ok(())
}

#[cfg(test)]
#[cfg(feature = "extension-sign")]
mod tests {
	use surrealdb::opt::Config;
	use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};
	use surrealdb_types::Value;

	use super::archive::fixture;
	use super::flow::{self, Options};
	use super::source::{BucketFile, Source};
	use super::*;

	/// The options of an install with `--unsigned`, for the unsigned
	/// fixture archives.
	fn unsigned() -> Options {
		Options {
			unsigned: true,
			..Options::default()
		}
	}

	async fn memory_db() -> Surreal<Any> {
		let mut caps = Capabilities::all();
		caps.allow_experimental_features(&[
			ExperimentalFeature::Files,
			ExperimentalFeature::Surrealism,
		]);
		let db =
			connect(("mem://", Config::default().capabilities(caps))).await.expect("mem engine");
		db.use_ns("test").use_db("test").await.expect("select ns/db");
		db.query("DEFINE BUCKET modules BACKEND \"memory\"")
			.await
			.expect("define bucket")
			.check()
			.expect("define bucket");
		db
	}

	fn modules(key: &str) -> BucketFile {
		BucketFile {
			bucket: "modules".into(),
			key: key.into(),
		}
	}

	async fn put(db: &Surreal<Any>, file: &BucketFile, bytes: &[u8]) {
		db.query("file::put($file, $bytes)")
			.bind(("file", file.to_value()))
			.bind(("bytes", Value::Bytes(bytes.to_vec().into())))
			.await
			.expect("put")
			.check()
			.expect("put");
	}

	#[tokio::test]
	async fn plans_an_install_from_a_bucket_file() {
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &fixture::archive()).await;

		let plan = flow::prepare_install(&db, &Source::Bucket(file), "color", &unsigned())
			.await
			.expect("plan");
		let digest = plan.decision.digest.clone();
		assert_eq!(
			plan.statement,
			format!(
				r#"DEFINE EXTENSION ext::color FROM f"modules:/color.surli"
    DIGEST '{digest}'
    UNSIGNED
    CAPABILITIES {{ net: ['api.color.io:443'], functions: ['http::get'], write: true }};"#
			)
		);
		let prompt = prompt::render_install(&plan);
		assert!(prompt.contains("acme/color 1.0.0 from f\"modules:/color.surli\""), "{prompt}");
		assert!(prompt.contains("⚠ UNSIGNED (--unsigned)"), "{prompt}");
	}

	#[tokio::test]
	async fn missing_bucket_file_is_reported() {
		let db = memory_db().await;
		let err = flow::prepare_install(
			&db,
			&Source::Bucket(modules("/absent.surli")),
			"color",
			&unsigned(),
		)
		.await
		.unwrap_err();
		assert!(format!("{err:#}").contains("does not exist"), "{err:#}");
	}

	#[tokio::test]
	async fn tampered_bucket_file_is_refused() {
		let db = memory_db().await;
		let file = modules("/bad.surli");
		let mut files = fixture::files();
		let manifest = fixture::manifest(&files);
		let swapped = fixture::PACKAGE_TOML.replace("api.color.io", "evil.example");
		files[1] = ("surrealism/package.toml", swapped.as_bytes());
		files.push((surrealism_runtime::package::MANIFEST_PATH, manifest.as_bytes()));
		put(&db, &file, &fixture::pack_raw(&files)).await;
		let err = flow::prepare_install(&db, &Source::Bucket(file), "color", &unsigned())
			.await
			.unwrap_err();
		assert!(format!("{err:#}").contains("does not match surrealism/MANIFEST"), "{err:#}");
	}

	/// Install and upgrade end to end: the server runs the statements and
	/// `INFO FOR DB STRUCTURE` reads them back.
	#[tokio::test]
	async fn installs_and_upgrades_end_to_end() {
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &fixture::archive()).await;

		let plan = flow::prepare_install(&db, &Source::Bucket(file.clone()), "color", &unsigned())
			.await
			.expect("plan");
		flow::commit(&db, &plan.statement).await.expect("install");

		let installed = installed::fetch_installed(&db, "color").await.expect("installed");
		assert_eq!(installed.digest.as_deref(), Some(plan.decision.digest.as_str()));
		assert_eq!(installed.capabilities, plan.decision.capabilities);
		assert_eq!(installed.signer, None);

		// Same file, same bytes: nothing to do.
		let same = flow::prepare_upgrade(&db, "color", None, &unsigned()).await.expect("plan");
		assert!(same.is_noop());

		// A new release in its own file with one more network target. The
		// installed file is still there, so the new target counts as new.
		let next = modules("/color-1.1.0.surli");
		put(&db, &next, &release("1.1.0", &["api.color.io:443", "cdn.color.io:443"])).await;
		let up = flow::prepare_upgrade(&db, "color", Some(&Source::Bucket(next)), &unsigned())
			.await
			.expect("plan");
		assert!(!up.is_noop());
		assert_eq!(up.signer_match, SignerMatch::BothUnsigned);
		assert!(up.statement.starts_with("DEFINE EXTENSION OVERWRITE ext::color"));
		flow::commit(&db, &up.statement).await.expect("upgrade");
		let after = installed::fetch_installed(&db, "color").await.expect("installed");
		assert_eq!(after.digest.as_deref(), Some(up.decision.digest.as_str()));
		assert!(after.capabilities.net.contains(&"cdn.color.io:443".to_string()));
	}

	/// The fixture archive at `version`, requesting `net`.
	fn release(version: &str, net: &[&str]) -> Vec<u8> {
		let net = net.iter().map(|n| format!("{n:?}")).collect::<Vec<_>>().join(", ");
		let toml = fixture::PACKAGE_TOML
			.replace("1.0.0", version)
			.replace(r#"["api.color.io:443"]"#, &format!("[{net}]"));
		let mut files = fixture::files();
		files[1] = ("surrealism/package.toml", toml.as_bytes());
		fixture::pack(&files, None)
	}

	/// An upgrade keeps PERMISSIONS and COMMENT, never re-grants what the
	/// install withheld, grants what is genuinely new, and refuses a downgrade
	/// unless asked.
	#[tokio::test]
	async fn an_upgrade_keeps_the_installed_decision() {
		let db = memory_db().await;
		let v1 = modules("/color-1.0.0.surli");
		put(&db, &v1, &release("1.0.0", &["api.color.io:443"])).await;
		let opts = Options {
			grant: Grant::With(vec!["net:api.color.io:443".into(), "functions:http::get".into()]),
			..unsigned()
		};
		let plan =
			flow::prepare_install(&db, &Source::Bucket(v1), "color", &opts).await.expect("plan");
		flow::commit(&db, &plan.statement).await.expect("install");
		db.query(
			"ALTER EXTENSION ext::color COMMENT 'palette' PERMISSIONS WHERE $auth.admin = true",
		)
		.await
		.unwrap()
		.check()
		.expect("alter");

		let v2 = modules("/color-1.1.0.surli");
		put(&db, &v2, &release("1.1.0", &["api.color.io:443", "cdn.color.io:443"])).await;
		let up = flow::prepare_upgrade(&db, "color", Some(&Source::Bucket(v2)), &unsigned())
			.await
			.expect("plan");
		assert!(!up.decision.capabilities.write, "withheld at install, still withheld");
		assert!(up.decision.capabilities.net.contains(&"cdn.color.io:443".to_string()));
		let text = prompt::render_upgrade(&up);
		assert!(text.contains("requested, not granted"), "{text}");
		flow::commit(&db, &up.statement).await.expect("upgrade");
		let after = installed::fetch_installed(&db, "color").await.expect("installed");
		assert_eq!(after.comment.as_deref(), Some("palette"));
		assert!(
			matches!(&after.permissions, statement::Permissions::Where(g) if g.contains("$auth.admin")),
			"{:?}",
			after.permissions
		);
		assert!(!after.capabilities.write);

		let v0 = modules("/color-0.9.0.surli");
		put(&db, &v0, &release("0.9.0", &["api.color.io:443"])).await;
		let back = Source::Bucket(v0);
		let err = flow::prepare_upgrade(&db, "color", Some(&back), &unsigned()).await.unwrap_err();
		assert!(err.to_string().contains("--allow-downgrade"), "{err}");
		assert!(err.to_string().contains("acme/color 1.1.0"), "{err}");
		let allowed = Options {
			allow_downgrade: true,
			..unsigned()
		};
		flow::prepare_upgrade(&db, "color", Some(&back), &allowed).await.expect("allowed");
	}

	/// A SIGNATURE stripped from an archive whose package.toml names a
	/// [signer] leaves DIGEST valid, but the install refuses it.
	#[tokio::test]
	async fn a_stripped_signature_is_refused() {
		let key = std::fs::read_to_string(
			crate::cli::module::sign::crosscheck::fixtures().join("ssh/id_ed25519.pub"),
		)
		.unwrap();
		let toml =
			format!("{}\n[signer.ssh]\npublic_key = \"{}\"\n", fixture::PACKAGE_TOML, key.trim());
		let mut files = fixture::files();
		files[1] = ("surrealism/package.toml", toml.as_bytes());
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &fixture::pack(&files, None)).await;
		let err =
			flow::prepare_install(&db, &Source::Bucket(file.clone()), "color", &Options::default())
				.await
				.unwrap_err();
		let err = format!("{err:#}");
		assert!(err.contains("package.toml names signer key SHA256:"), "{err}");
		assert!(err.contains("carries no SIGNATURE (it may have been removed)"), "{err}");
		assert!(err.contains("Pass --unsigned"), "{err}");

		// With --unsigned it installs UNSIGNED, and the prompt says the
		// declared signer was not verified.
		let plan = flow::prepare_install(&db, &Source::Bucket(file), "color", &unsigned())
			.await
			.expect("plan");
		assert!(plan.statement.contains("\n    UNSIGNED\n"), "{}", plan.statement);
		let prompt = prompt::render_install(&plan);
		assert!(prompt.contains("declared signer key SHA256:"), "{prompt}");
		assert!(prompt.contains("not verified (--unsigned)"), "{prompt}");
	}

	/// An archive with neither [signer] nor SIGNATURE needs --unsigned.
	#[tokio::test]
	async fn an_unsigned_archive_needs_the_unsigned_flag() {
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &fixture::archive()).await;
		let err =
			flow::prepare_install(&db, &Source::Bucket(file.clone()), "color", &Options::default())
				.await
				.unwrap_err();
		assert_eq!(err.to_string(), "this extension is not signed; pass --unsigned to install it");
		flow::prepare_install(&db, &Source::Bucket(file), "color", &unsigned())
			.await
			.expect("--unsigned installs it");
	}

	/// --unsigned never reads the signature, even a present one: here one
	/// that would not verify.
	#[tokio::test]
	async fn unsigned_skips_a_present_signature() {
		let key = std::fs::read_to_string(
			crate::cli::module::sign::crosscheck::fixtures().join("ssh/id_ed25519.pub"),
		)
		.unwrap();
		let toml =
			format!("{}\n[signer.ssh]\npublic_key = \"{}\"\n", fixture::PACKAGE_TOML, key.trim());
		let mut files = fixture::files();
		files[1] = ("surrealism/package.toml", toml.as_bytes());
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &fixture::pack(&files, Some(b"-----BEGIN SSH SIGNATURE-----\nnot one\n")))
			.await;
		assert!(
			flow::prepare_install(&db, &Source::Bucket(file.clone()), "color", &Options::default())
				.await
				.is_err(),
			"without --unsigned the bad signature is checked"
		);
		let plan = flow::prepare_install(&db, &Source::Bucket(file), "color", &unsigned())
			.await
			.expect("--unsigned does not read it");
		assert_eq!(plan.decision.signer, None);
		flow::commit(&db, &plan.statement).await.expect("the server accepts UNSIGNED");
	}

	/// `upgrade --with-capability` adds to the installed grant: what the install
	/// withheld stays withheld unless the grant names it.
	#[tokio::test]
	async fn an_upgrade_grant_does_not_restore_what_install_withheld() {
		let db = memory_db().await;
		let v1 = modules("/color-1.0.0.surli");
		put(&db, &v1, &release("1.0.0", &["api.color.io:443"])).await;
		let withheld = Options {
			grant: Grant::With(vec!["net:api.color.io:443".into(), "functions:http::get".into()]),
			..unsigned()
		};
		let plan = flow::prepare_install(&db, &Source::Bucket(v1), "color", &withheld)
			.await
			.expect("plan");
		flow::commit(&db, &plan.statement).await.expect("install");

		let v2 = modules("/color-1.1.0.surli");
		put(&db, &v2, &release("1.1.0", &["api.color.io:443", "cdn.color.io:443"])).await;
		let narrow = Options {
			grant: Grant::With(vec!["net:api.color.io:443".into()]),
			..unsigned()
		};
		let up = flow::prepare_upgrade(&db, "color", Some(&Source::Bucket(v2)), &narrow)
			.await
			.expect("plan");
		assert!(!up.decision.capabilities.write, "write stays withheld");
		assert_eq!(up.decision.capabilities.net, vec!["api.color.io:443".to_string()]);
	}

	/// A bucket upgrade whose installed file was replaced cannot tell a
	/// downgrade or a package change, and says so.
	#[tokio::test]
	async fn an_unreadable_installed_package_is_reported() {
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &release("1.1.0", &["api.color.io:443"])).await;
		let plan = flow::prepare_install(&db, &Source::Bucket(file.clone()), "color", &unsigned())
			.await
			.expect("plan");
		flow::commit(&db, &plan.statement).await.expect("install");
		put(&db, &file, &release("0.9.0", &["api.color.io:443"])).await;
		let up = flow::prepare_upgrade(&db, "color", None, &unsigned()).await.expect("plan");
		assert!(!up.package_known, "the installed archive is gone, so nothing was checked");
	}

	/// Moving to another package is refused unless --allow-package-change.
	#[tokio::test]
	async fn a_package_change_needs_its_flag() {
		let db = memory_db().await;
		let v1 = modules("/color-1.0.0.surli");
		put(&db, &v1, &release("1.0.0", &["api.color.io:443"])).await;
		let plan = flow::prepare_install(&db, &Source::Bucket(v1), "color", &unsigned())
			.await
			.expect("plan");
		flow::commit(&db, &plan.statement).await.expect("install");

		let toml = fixture::PACKAGE_TOML.replace("name = \"color\"", "name = \"palette\"");
		let mut files = fixture::files();
		files[1] = ("surrealism/package.toml", toml.as_bytes());
		let other = modules("/palette-1.0.0.surli");
		put(&db, &other, &fixture::pack(&files, None)).await;
		let source = Source::Bucket(other);
		let err =
			flow::prepare_upgrade(&db, "color", Some(&source), &unsigned()).await.unwrap_err();
		assert!(err.to_string().contains("--allow-package-change"), "{err}");
		assert!(err.to_string().contains("acme/palette"), "{err}");
		let allowed = Options {
			allow_package_change: true,
			..unsigned()
		};
		let up =
			flow::prepare_upgrade(&db, "color", Some(&source), &allowed).await.expect("allowed");
		assert!(up.package_known);
	}

	/// A changed signer is answered only by --allow-signer-change, and the
	/// flag names itself in the output.
	#[test]
	fn a_signer_change_needs_its_own_flag() {
		let different = SignerMatch::Different(vec!["identity changed".into()]);
		let err = confirm_signer_change(&different, false).unwrap_err();
		assert!(err.to_string().contains("--allow-signer-change"), "{err}");
		confirm_signer_change(&different, true).expect("allowed by the flag");
		confirm_signer_change(&SignerMatch::Same, false).expect("nothing to decide");
	}

	/// Without a terminal, a signed install needs --trust-signer, and one that
	/// names another signer refuses the archive. So a Silo that serves an
	/// archive signed by someone else (unregistered, or not the one expected)
	/// cannot be installed by an automated run that pins its signer.
	#[test]
	fn an_unexpected_signer_is_refused_without_a_question() {
		let attacker = signer::Signer::Sigstore(signer::SigstoreSigner {
			ca: vec!["Q0E=".into()],
			log: "TE9H".into(),
			tsa: vec!["VFNB".into()],
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: "https://github.com/acm3/color/.github/workflows/release.yml@refs/tags/v1"
				.into(),
			repository_id: None,
			owner_id: None,
		});
		let expected: TrustSigner =
			"https://github.com/acme/color/.github/workflows/release.yml".parse().unwrap();
		let err = confirm_signer(Some(&attacker), Some(&expected)).unwrap_err();
		assert!(err.to_string().contains("not the signer --trust-signer names"), "{err}");
		let err = confirm_signer(Some(&attacker), None).unwrap_err();
		assert!(err.to_string().contains("--trust-signer"), "{err}");
		confirm_signer(Some(&attacker), Some(&TrustSigner::Any)).expect("`any` is explicit");
		confirm_signer(None, None).expect("--unsigned has no signer to decide");
	}

	/// Without --with-capability and without a terminal, an install grants
	/// nothing that needs approval.
	#[tokio::test]
	async fn a_non_interactive_install_grants_nothing_unasked() {
		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &fixture::archive()).await;
		let opts = Options {
			grant: Grant::Nothing,
			..unsigned()
		};
		let plan =
			flow::prepare_install(&db, &Source::Bucket(file), "color", &opts).await.expect("plan");
		assert!(plan.statement.contains("CAPABILITIES {}"), "{}", plan.statement);
		let text = prompt::render_install(&plan);
		assert!(text.contains("requested, not granted"), "{text}");
	}

	/// The public key line `ssh-keygen` wrote next to `key`, without its
	/// comment: the form a `SIGNER { key }` clause records.
	fn public_key(key: &std::path::Path) -> String {
		let line = std::fs::read_to_string(key.with_extension("pub")).expect("read the public key");
		line.split_whitespace().take(2).collect::<Vec<_>>().join(" ")
	}

	/// Build, sign with an SSH key as `surreal extension sign --key` does,
	/// install from a bucket file, and let the server verify the statement.
	#[tokio::test]
	async fn installs_an_ssh_signed_archive_the_server_verifies() {
		use crate::cli::module::sign::{self, ssh};
		if !ssh::tests::have_ssh_keygen() {
			eprintln!("skipping: ssh-keygen not on PATH");
			return;
		}
		let dir = tempfile::tempdir().expect("tempdir");
		let key = ssh::tests::generate_key(dir.path());
		let plain = dir.path().join("color.surli");
		let signed = dir.path().join("color-signed.surli");
		// The package names the key as its [signer], as `extension config
		// signer ssh` writes it.
		let toml = format!(
			"{}\n[signer.ssh]\npublic_key = \"{}\"\n",
			fixture::PACKAGE_TOML,
			public_key(&key)
		);
		let mut files = fixture::files();
		files[1] = ("surrealism/package.toml", toml.as_bytes());
		std::fs::write(&plain, fixture::pack(&files, None)).expect("write the archive");
		let digest =
			sign::sign(&plain, &signed, sign::Source::Key(&key), None).await.expect("sign");

		let db = memory_db().await;
		let file = modules("/color.surli");
		put(&db, &file, &std::fs::read(&signed).expect("read the signed archive")).await;

		let plan = flow::prepare_install(&db, &Source::Bucket(file), "color", &Options::default())
			.await
			.expect("plan");
		let key_line = public_key(&key);
		assert_eq!(plan.decision.digest, digest);
		assert_eq!(
			plan.statement,
			format!(
				r#"DEFINE EXTENSION ext::color FROM f"modules:/color.surli"
    DIGEST '{digest}'
    SIGNER {{ key: '{key_line}' }}
    CAPABILITIES {{ net: ['api.color.io:443'], functions: ['http::get'], write: true }};"#
			)
		);
		let prompt = prompt::render_install(&plan);
		assert!(prompt.contains("Signed by   key ssh-ed25519 SHA256:"), "{prompt}");

		flow::commit(&db, &plan.statement).await.expect("the server verifies the signer");
		let installed = installed::fetch_installed(&db, "color").await.expect("installed");
		assert_eq!(installed.signer, Some(signer::Signer::Key(key_line)));
		assert_eq!(installed.digest.as_deref(), Some(digest.as_str()));

		// The same archive under another key's SIGNER is refused by the server.
		let other_dir = tempfile::tempdir().expect("tempdir");
		let other = ssh::tests::generate_key(other_dir.path());
		let forged = statement::replace(&statement::Decision {
			signer: Some(signer::Signer::Key(public_key(&other))),
			..plan.decision.clone()
		})
		.expect("statement");
		let err = flow::commit(&db, &forged).await.unwrap_err();
		assert!(format!("{err:#}").contains("SIGNATURE does not prove its SIGNER"), "{err:#}");

		// An UNSIGNED install becomes SIGNER only through an upgrade that
		// verifies: --unsigned writes UNSIGNED, a plain upgrade the signer.
		db.query("REMOVE EXTENSION ext::color").await.unwrap().check().unwrap();
		let plan = flow::prepare_install(
			&db,
			&Source::Bucket(modules("/color.surli")),
			"color",
			&unsigned(),
		)
		.await
		.expect("plan");
		flow::commit(&db, &plan.statement).await.expect("install UNSIGNED");
		let still = flow::prepare_upgrade(&db, "color", None, &unsigned()).await.expect("plan");
		assert_eq!(still.decision.signer, None, "--unsigned keeps it UNSIGNED");
		let up =
			flow::prepare_upgrade(&db, "color", None, &Options::default()).await.expect("plan");
		assert_eq!(up.decision.signer, Some(signer::Signer::Key(public_key(&key))));
		flow::commit(&db, &up.statement).await.expect("the server verifies the new SIGNER");
		let installed = installed::fetch_installed(&db, "color").await.expect("installed");
		assert_eq!(installed.signer, Some(signer::Signer::Key(public_key(&key))));
	}

	#[test]
	fn debug_output_carries_no_credentials() {
		use clap::Parser as _;
		#[derive(clap::Parser)]
		struct Cli {
			#[command(flatten)]
			args: InstallArgs,
		}
		let cli = Cli::parse_from([
			"install",
			"silo::acme::color",
			"--as",
			"color",
			"-u",
			"root",
			"-p",
			"hunter2",
			"--ns",
			"n",
			"--db",
			"d",
		]);
		let shown = format!("{:?}", cli.args);
		assert!(!shown.contains("hunter2"), "{shown}");
		assert!(shown.contains("color"), "{shown}");
	}
}
