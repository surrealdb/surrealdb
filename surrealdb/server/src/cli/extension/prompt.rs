//! What the person sees before approving an install or an upgrade.

use std::fmt::Write as _;

use surrealism_runtime::config::PackageSigner;

use super::archive::Export;
use super::capabilities::{Capabilities, Change, field_label};
use super::flow::{Inspected, InstallPlan, Origin, SigstoreChecks, UpgradePlan};
use super::online::Check;
use super::signer::{self, Signer, SignerMatch};
use super::source::DefinedSource;
use super::sql;

/// Width of the label column, as in `  Package     acme/color …`.
const LABEL: usize = 12;

/// `s` with every character that could rewrite the terminal or reorder text
/// escaped as `\u{…}`: C0 and C1 controls, DEL, and the bidirectional
/// formatting characters. Everything printed from an archive or from Silo
/// goes through it.
pub fn clean(s: &str) -> String {
	let mut out = String::with_capacity(s.len());
	for c in s.chars() {
		let bidi = matches!(
			c,
			'\u{061C}' | '\u{200E}' | '\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}'
		);
		if c.is_control() || bidi {
			let _ = write!(out, "\\u{{{:04x}}}", c as u32);
		} else {
			out.push(c);
		}
	}
	out
}

fn row(out: &mut String, label: &str, value: &str) {
	let _ = writeln!(out, "  {label:<LABEL$}{}", clean(value));
}

/// A free-form line, escaped as [`row`] escapes its value.
fn line(out: &mut String, text: &str) {
	let _ = writeln!(out, "{}", clean(text));
}

fn cont(out: &mut String, value: &str) {
	row(out, "", value);
}

fn package_line(inspected: &Inspected) -> String {
	let f = &inspected.facts;
	let pkg = format!("{}/{} {}", f.organisation, f.name, f.version);
	match &inspected.origin {
		Origin::Silo {
			..
		} => format!("{pkg} on Silo"),
		Origin::Bucket(file) => format!("{pkg} from {file}"),
	}
}

/// One line per check of a Sigstore signature; anything but ✓ stands out.
fn check_lines(checks: &SigstoreChecks) -> Vec<String> {
	let offline = match &checks.trust_root {
		None => "✓ signature verifies against the archive's trust root (offline)".to_string(),
		Some(path) => {
			format!("✓ signature verifies against --trust-root {} (offline)", path.display())
		}
	};
	let root = match &checks.online.root {
		Check::Passed => "✓ trust root is Sigstore public-good (checked online)".to_string(),
		Check::NotChecked(why) => format!("⚠ could not check the trust root online: {why}"),
		Check::Failed(why) => format!("✗ {why}"),
	};
	let log = match &checks.online.log {
		// The served entry is proved in the tree a checkpoint signed by the
		// public log key commits to; see `online::log_entry`.
		Check::Passed => "✓ entry in the public Rekor log".to_string(),
		Check::NotChecked(why) => format!("⚠ log entry not checked: {why}"),
		Check::Failed(why) => format!("✗ the public log does not hold this entry: {why}"),
	};
	vec![offline, root, log]
}

/// The signer `package.toml` names, which the proven signer has matched.
fn declared(out: &mut String, inspected: &Inspected) {
	if inspected.unsigned {
		if let Some(declared) = &inspected.facts.declared_signer {
			cont(
				out,
				&format!(
					"⚠ declared signer {} — not verified (--unsigned)",
					signer::describe_declared(declared)
				),
			);
		}
		return;
	}
	match (&inspected.facts.declared_signer, &inspected.signer) {
		(Some(PackageSigner::Sigstore(s)), _) => {
			let ids = [("repository", &s.repository_id), ("owner", &s.owner_id)]
				.iter()
				.filter_map(|(what, id)| id.as_ref().map(|id| format!("{what} {id}")))
				.collect::<Vec<_>>();
			let ids = if ids.is_empty() {
				String::new()
			} else {
				format!(", {}", ids.join(", "))
			};
			cont(
				out,
				&format!(
					"✓ matches package.toml [signer]: {}{ids}",
					signer::describe_identity(&s.identity)
				),
			);
		}
		(Some(PackageSigner::Ssh(s)), _) => cont(
			out,
			&format!(
				"✓ matches package.toml [signer]: key {}",
				signer::ssh_key_fingerprint(&s.public_key)
			),
		),
		// Refused before the prompt: a signed archive must name its signer.
		(None, _) => {}
	}
}

/// The checks that sit under `Signed by`: how a Sigstore signature was
/// checked, the package's declared signer, Silo's publisher record or the
/// declared repository, and Silo's release status.
fn provenance(out: &mut String, inspected: &Inspected) {
	if let Some(checks) = &inspected.checks {
		for line in check_lines(checks) {
			cont(out, &line);
		}
	}
	declared(out, inspected);
	let f = &inspected.facts;
	if let Origin::Silo {
		details,
		warnings,
	} = &inspected.origin
	{
		for warning in warnings {
			cont(out, &format!("⚠ {warning}"));
		}
		let Some(details) = details else {
			return;
		};
		// Silo's publisher record speaks only for the signer it names, so it
		// is shown only when that is exactly the signer the signature proved.
		let Some(listed) = &details.signer else {
			return;
		};
		match &inspected.signer {
			Some(proved) if listed.matches(proved) => {
				if listed.registered_publisher {
					cont(out, &format!("✓ registered publisher of \"{}\" on Silo", f.organisation))
				} else {
					cont(
						out,
						&format!("⚠ NOT a registered publisher of \"{}\" on Silo", f.organisation),
					)
				}
			}
			_ => {
				cont(out, &format!("⚠ Silo lists a DIFFERENT signer: {}", listed.describe()));
				cont(out, "  Silo's publisher record does not apply to the signer shown above");
			}
		}
		return;
	}
	let (Some(Signer::Sigstore(s)), Some(declared)) = (&inspected.signer, &f.repository) else {
		return;
	};
	let declared_short = declared.trim_start_matches("https://").trim_end_matches('/');
	match signer::identity_repository(&s.identity) {
		Some(repo) if repo.trim_end_matches('/') == declared.trim_end_matches('/') => {
			cont(out, &format!("✓ matches declared repository {declared_short}"))
		}
		_ => cont(out, &format!("⚠ does NOT match declared repository {declared_short}")),
	}
}

fn adds(name: &str, exports: &[Export]) -> String {
	exports
		.iter()
		.map(|e| {
			let path = match &e.name {
				Some(n) => format!("ext::{}::{}", sql::segment(name), sql::segment(n)),
				None => format!("ext::{}", sql::segment(name)),
			};
			if e.writes {
				format!("{path} (writes)")
			} else {
				path
			}
		})
		.collect::<Vec<_>>()
		.join(", ")
}

fn signer_rows(out: &mut String, inspected: &Inspected) {
	let rows = signer::prompt_rows(inspected.signer.as_ref(), inspected.anchor);
	let mut rows = rows.into_iter();
	if let Some((label, value)) = rows.next() {
		row(out, label, &value);
	}
	provenance(out, inspected);
	for (label, value) in rows {
		row(out, label, &value);
	}
}

/// One line per item the archive requests but the statement does not grant.
/// Returns whether there was any.
fn withheld(out: &mut String, requests: &Capabilities, granted: &Capabilities) -> bool {
	let mut any = false;
	for c in requests.diff(granted) {
		if let Change::Added {
			field,
			value,
		} = c
		{
			any = true;
			line(
				out,
				&format!(
					"  · {:<10}{value:<36}requested, not granted (--with-capability grants it)",
					field_label(field)
				),
			);
		}
	}
	any
}

pub fn render_install(plan: &InstallPlan) -> String {
	let mut out = String::new();
	let i = &plan.inspected;
	row(&mut out, "Package", &package_line(i));
	signer_rows(&mut out, i);
	row(&mut out, "Digest", &plan.decision.digest);
	row(&mut out, "Adds", &adds(&plan.decision.name, &i.facts.exports));
	let caps = plan.decision.capabilities.prompt_rows();
	if caps.is_empty() {
		row(&mut out, "Needs", "nothing");
	}
	for (n, (label, value)) in caps.iter().enumerate() {
		let first = if n == 0 {
			"Needs"
		} else {
			""
		};
		row(&mut out, first, &format!("{label:<12}{value}"));
	}
	withheld(&mut out, &i.facts.requests, &plan.decision.capabilities);
	out
}

pub fn render_upgrade(plan: &UpgradePlan) -> String {
	let mut out = String::new();
	let name = &plan.decision.name;
	let from = match &plan.installed.source {
		DefinedSource::Silo {
			version,
			..
		} => version.to_string(),
		other => other.to_string(),
	};
	let to = match &plan.decision.source {
		DefinedSource::Silo {
			version,
			..
		} => version.to_string(),
		other => other.to_string(),
	};
	line(&mut out, &format!("Upgrade ext::{name} {from} → {to}"));
	let i = &plan.inspected;
	row(&mut out, "Package", &package_line(i));

	match &plan.signer_match {
		SignerMatch::Same => {
			let who = signer::prompt_rows(i.signer.as_ref(), i.anchor)
				.into_iter()
				.next()
				.map(|(_, v)| v)
				.unwrap_or_default();
			row(&mut out, "Signed by", &format!("{who}   ✓ same signer as installed version"));
			provenance(&mut out, i);
		}
		SignerMatch::BothUnsigned => {
			row(&mut out, "Signed by", "⚠ UNSIGNED (--unsigned), as the installed version");
			provenance(&mut out, i);
		}
		SignerMatch::Different(reasons) => {
			let _ = writeln!(out);
			let _ = writeln!(out, "  ⚠⚠⚠ DIFFERENT signer than installed version ⚠⚠⚠");
			for r in reasons {
				line(&mut out, &format!("      {r}"));
			}
			let _ = writeln!(out);
			signer_rows(&mut out, i);
		}
	}
	let old_digest = plan.installed.digest.as_deref().unwrap_or("none recorded");
	if old_digest == plan.decision.digest {
		row(&mut out, "Digest", &format!("{} (unchanged)", plan.decision.digest));
	} else {
		row(&mut out, "Digest", &format!("{old_digest} → {}", plan.decision.digest));
	}

	row(&mut out, "Permissions", &format!("{} (kept)", plan.decision.permissions_sql()));

	let changes = plan.decision.capabilities.diff(&plan.installed.capabilities);
	if changes.is_empty() {
		row(&mut out, "Needs", "no change");
	}
	for c in changes {
		let text = match c {
			Change::Added {
				field,
				value,
			} => format!("+ {:<10}{value:<36}← new, needs approval", field_label(field)),
			Change::Removed {
				field,
				value,
			} => format!("- {:<10}{value:<36}(no longer granted)", field_label(field)),
			Change::Changed {
				field,
				from,
				to,
			} => format!("~ {:<10}{from} → {to}", field_label(field)),
		};
		line(&mut out, &format!("  {text}"));
	}
	// What the archive requests but this upgrade does not grant: withheld at
	// install, or new while the installed archive could not be read.
	let withheld = withheld(&mut out, &plan.inspected.facts.requests, &plan.decision.capabilities);
	if plan.previous.is_none() && withheld {
		cont(
			&mut out,
			"(the installed archive could not be read again, so no request counts as new)",
		);
	}
	out
}

#[cfg(test)]
mod tests {
	use semver::Version;

	use super::*;
	use crate::cli::extension::archive::ArchiveFacts;
	use crate::cli::extension::capabilities::Capabilities;
	use crate::cli::extension::installed::Installed;
	use crate::cli::extension::signer::{AnchorLabel, SigstoreSigner};
	use crate::cli::extension::silo::{self, PackageDetails};
	use crate::cli::extension::statement::{self, Decision};

	const GH: &str = "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0";

	fn signer(identity: &str) -> Signer {
		Signer::Sigstore(SigstoreSigner {
			ca: vec!["Uk9PVA==".into()],
			log: "TE9H".into(),
			tsa: vec![],
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: identity.into(),
			repository_id: Some("812345678".into()),
			owner_id: Some("98765432".into()),
		})
	}

	fn export(name: &str, writes: bool) -> Export {
		Export {
			name: Some(name.into()),
			args: vec![],
			returns: "string".into(),
			writes,
			comment: None,
		}
	}

	fn inspected(version: Version, signer: Option<Signer>, requests: Capabilities) -> Inspected {
		Inspected {
			facts: ArchiveFacts {
				digest: surrealism_runtime::package::Digest([0x5d; 32]),
				signature: None,
				organisation: "acme".into(),
				name: "color".into(),
				version,
				repository: Some("https://github.com/acme/color".into()),
				declared_signer: Some(PackageSigner::Sigstore(
					surrealism_runtime::config::SigstoreSigner {
						issuer: "https://token.actions.githubusercontent.com".into(),
						identity: "https://github.com/acme/color/.github/workflows/release.yml"
							.into(),
						repository_id: Some("812345678".into()),
						owner_id: Some("98765432".into()),
					},
				)),
				requests,
				exports: vec![export("blend", false), export("recolor", true)],
			},
			origin: Origin::Silo {
				details: Some(Box::new(PackageDetails {
					digest: silo::ManifestDigest::try_from(format!("sha256:{}", "5d".repeat(32)))
						.unwrap(),
					signer: Some(silo::Signer {
						issuer: Some("https://token.actions.githubusercontent.com".into()),
						identity: Some(GH.into()),
						repository_id: Some("812345678".into()),
						owner_id: Some("98765432".into()),
						key: None,
						registered_publisher: true,
					}),
					yanked: false,
					advisories: vec![],
				})),
				warnings: vec![],
			},
			signer,
			anchor: AnchorLabel::PublicGood,
			checks: None,
			unsigned: false,
		}
	}

	fn requests() -> Capabilities {
		Capabilities {
			net: vec!["api.color.io:443".into()],
			functions: vec!["http::get".into()],
			write: true,
			..Capabilities::default()
		}
	}

	fn silo(version: Version) -> DefinedSource {
		DefinedSource::Silo {
			org: "acme".into(),
			pkg: "color".into(),
			version,
		}
	}

	#[test]
	fn install_prompt() {
		let i = inspected(Version::new(1, 0, 0), Some(signer(GH)), requests());
		let decision = Decision {
			name: "color".into(),
			source: silo(Version::new(1, 0, 0)),
			digest: "sha256:5d1a".into(),
			signer: i.signer.clone(),
			capabilities: requests(),
			permissions: None,
			comment: None,
		};
		let plan = InstallPlan {
			statement: statement::define(&decision).unwrap(),
			inspected: i,
			decision,
		};
		let ca = signer::der_fingerprint("Uk9PVA==");
		let log = signer::der_fingerprint("TE9H");
		assert_eq!(
			render_install(&plan),
			format!(
				"  Package     acme/color 1.0.0 on Silo
  Signed by   github.com/acme/color  (.github/workflows/release.yml @ v1.0.0)
              ✓ matches package.toml [signer]: github.com/acme/color  (.github/workflows/release.yml), repository 812345678, owner 98765432
              ✓ registered publisher of \"acme\" on Silo
  IDs         repository 812345678, owner 98765432
  Issuer      https://token.actions.githubusercontent.com
  Anchor      Sigstore public-good   (ca {ca}, log {log})
  Digest      sha256:5d1a
  Adds        ext::color::blend, ext::color::recolor (writes)
  Needs       network     api.color.io:443
              functions   http::get
              write       yes
"
			)
		);
	}

	fn upgrade(new_identity: &str, new_caps: Capabilities) -> UpgradePlan {
		let i = inspected(Version::new(1, 1, 0), Some(signer(new_identity)), new_caps.clone());
		let decision = Decision {
			name: "color".into(),
			source: silo(Version::new(1, 1, 0)),
			digest: "sha256:77f0".into(),
			signer: i.signer.clone(),
			capabilities: new_caps,
			permissions: Some(statement::Permissions::Full),
			comment: None,
		};
		let installed = Installed {
			source: silo(Version::new(1, 0, 0)),
			digest: Some("sha256:5d1a".into()),
			signer: Some(signer(GH)),
			capabilities: requests(),
			permissions: statement::Permissions::Full,
			comment: None,
		};
		UpgradePlan {
			package_known: true,
			previous: None,
			signer_match: signer::compare(installed.signer.as_ref(), decision.signer.as_ref()),
			statement: statement::replace(&decision).unwrap(),
			installed,
			inspected: i,
			decision,
		}
	}

	#[test]
	fn upgrade_prompt_same_signer_new_network() {
		let mut caps = requests();
		caps.net.push("cdn.color.io:443".into());
		let plan = upgrade(
			"https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.1.0",
			caps,
		);
		let text = render_upgrade(&plan);
		assert!(text.starts_with("Upgrade ext::color 1.0.0 → 1.1.0\n"), "{text}");
		assert!(text.contains("✓ same signer as installed version"), "{text}");
		assert!(text.contains("+ network   cdn.color.io:443"), "{text}");
		assert!(text.contains("← new, needs approval"), "{text}");
		assert!(text.contains("sha256:5d1a → sha256:77f0"), "{text}");
		assert!(!plan.is_noop());
	}

	#[test]
	fn upgrade_prompt_different_signer_is_loud() {
		let plan = upgrade(
			"https://github.com/acm3/color/.github/workflows/release.yml@refs/tags/v1.2.0",
			requests(),
		);
		let text = render_upgrade(&plan);
		assert!(text.contains("DIFFERENT signer than installed version"), "{text}");
		assert!(text.contains("identity https://github.com/acme/color/.github/workflows/release.yml → https://github.com/acm3/color/.github/workflows/release.yml"), "{text}");
		assert!(text.contains("Needs       no change"), "{text}");
	}

	#[test]
	fn control_and_bidi_characters_are_escaped() {
		assert_eq!(clean("a\u{1b}[2Jb"), "a\\u{001b}[2Jb");
		assert_eq!(clean("x\u{202e}gpj.exe"), "x\\u{202e}gpj.exe");
		assert_eq!(clean("\u{9b}31m"), "\\u{009b}31m");
		assert_eq!(clean("✓ plain"), "✓ plain");

		let mut caps = requests();
		caps.net = vec!["evil\u{1b}]8;;x\u{7}.io:443".into()];
		let mut i = inspected(Version::new(1, 0, 0), Some(signer(GH)), caps.clone());
		i.facts.organisation = "ac\u{202e}me".into();
		let decision = Decision {
			name: "color".into(),
			source: silo(Version::new(1, 0, 0)),
			digest: "sha256:5d1a".into(),
			signer: i.signer.clone(),
			capabilities: caps,
			permissions: None,
			comment: None,
		};
		let plan = InstallPlan {
			statement: String::new(),
			inspected: i,
			decision,
		};
		let text = render_install(&plan);
		assert!(!text.chars().any(|c| c.is_control() && c != '\n'), "{text:?}");
		assert!(!text.contains('\u{202e}'), "{text:?}");
	}

	#[test]
	fn the_publisher_line_needs_the_proved_signer() {
		// Silo lists another identity than the signature proves.
		let other = "https://github.com/acm3/color/.github/workflows/release.yml@refs/tags/v1.0.0";
		let i = inspected(Version::new(1, 0, 0), Some(signer(other)), requests());
		let mut text = String::new();
		provenance(&mut text, &i);
		assert!(text.contains("Silo lists a DIFFERENT signer"), "{text}");
		assert!(!text.contains("registered publisher of"), "{text}");

		// An SSH signer matches on key type and blob.
		let key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w";
		let mut i = inspected(Version::new(1, 0, 0), Some(Signer::Key(key.into())), requests());
		if let Origin::Silo {
			details: Some(details),
			..
		} = &mut i.origin
		{
			details.signer = Some(silo::Signer {
				key: Some(format!("{key} release@acme")),
				registered_publisher: true,
				..silo::Signer::default()
			});
		}
		let mut text = String::new();
		provenance(&mut text, &i);
		assert!(text.contains("✓ registered publisher of \"acme\""), "{text}");
	}

	#[test]
	fn silo_warnings_show_for_the_same_signer() {
		let mut plan = upgrade(GH, requests());
		if let Origin::Silo {
			warnings,
			..
		} = &mut plan.inspected.origin
		{
			warnings.push("silo::acme::color<1.1.0> has been yanked by its publisher".into());
		}
		let text = render_upgrade(&plan);
		assert!(text.contains("same signer as installed version"), "{text}");
		assert!(text.contains("⚠ silo::acme::color<1.1.0> has been yanked"), "{text}");
		assert!(text.contains("✓ registered publisher"), "{text}");
	}
}
