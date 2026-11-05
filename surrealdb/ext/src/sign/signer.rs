//! The `[signer]` a package names, and whether a signature comes from it.
//!
//! `sign` reads `[signer]` from the `package.toml` inside the archive, which
//! the archive's `MANIFEST` covers, and refuses to write a signature whose
//! certificate or key is anyone else's. Without `[signer]` it refuses to sign
//! at all.

use std::path::Path;

use anyhow::{Result, bail};
use surrealism_runtime::config::{PackageSigner, SigstoreSigner, SshSigner};
use surrealism_verify::Claims;

use super::archive;

/// The `[signer]` of the package in the archive at `path`.
pub fn of_archive(path: &Path) -> Result<PackageSigner> {
	match archive::read_package(path)?.config().signer.clone() {
		Some(signer) => Ok(signer),
		None => bail!(
			"The package has no [signer], so there is nothing to sign as. Name one in the \
			 project with `surreal ext config signer github|gitlab|ssh …`, rebuild with \
			 `surreal ext build`, and sign again"
		),
	}
}

/// Refuse `signature` unless it claims to come from `signer`.
///
/// The claims are read without being proven; whoever installs the archive
/// proves them. This check only stops a CI job or a key from signing as a
/// signer the package does not name.
pub fn ensure_matches(signature: &[u8], signer: &PackageSigner) -> Result<()> {
	let claims = surrealism_verify::claims(signature)
		.map_err(|e| anyhow::anyhow!("Cannot read who the signature claims to come from: {e}"))?;
	check(&claims, signer)
}

/// Refuse an OIDC identity token whose claims say the certificate Fulcio
/// would issue for it is not `signer`'s, before any certificate is asked for.
///
/// Fulcio writes a GitHub Actions token's `job_workflow_ref` into the
/// identity as `https://github.com/<ref>`, and a GitLab CI token's
/// `ci_config_ref_uri` as `https://<uri>`; the repository and owner ids come
/// from `repository_id` / `repository_owner_id` and `project_id` /
/// `namespace_id`. For another provider only the issuer is checked here;
/// the certificate is checked in full once it is issued.
pub fn ensure_token_matches(claims: &serde_json::Value, signer: &PackageSigner) -> Result<()> {
	let PackageSigner::Sigstore(expected) = signer else {
		bail!("[signer] names an SSH key, so the package is signed with `--key`, not keyless");
	};
	let text = |value: &serde_json::Value| match value {
		serde_json::Value::String(s) => Some(s.clone()),
		serde_json::Value::Number(n) => Some(n.to_string()),
		serde_json::Value::Null
		| serde_json::Value::Bool(_)
		| serde_json::Value::Array(_)
		| serde_json::Value::Object(_) => None,
	};
	let issuer = claims["iss"].as_str().unwrap_or_default();
	let (identity, repository_id, owner_id) = if let Some(workflow) =
		claims["job_workflow_ref"].as_str()
	{
		(
			format!("https://github.com/{workflow}"),
			text(&claims["repository_id"]),
			text(&claims["repository_owner_id"]),
		)
	} else if let Some(config) = claims["ci_config_ref_uri"].as_str() {
		(format!("https://{config}"), text(&claims["project_id"]), text(&claims["namespace_id"]))
	} else {
		if issuer != expected.issuer {
			bail!(
				"The identity token's issuer is {issuer}, but [signer.sigstore] names {}",
				expected.issuer
			);
		}
		return Ok(());
	};
	check_sigstore(issuer, &identity, &repository_id, &owner_id, expected)
		.map_err(|e| anyhow::anyhow!("Not requesting a certificate: {e}"))
}

/// The identity with its `@<ref>` suffix removed, so that every ref of one
/// workflow compares equal: everything from the first `@` after the URL's
/// path starts. An identity without a scheme (an e-mail address) or without
/// a path has no ref and is returned unchanged.
///
/// `surreal ext sign`, `install` and `upgrade` all use this one rule.
pub(crate) fn identity_without_ref(identity: &str) -> &str {
	let Some(scheme_end) = identity.find("://") else {
		return identity;
	};
	let after_scheme = scheme_end + 3;
	let Some(path_start) = identity[after_scheme..].find('/').map(|i| after_scheme + i) else {
		return identity;
	};
	match identity[path_start..].find('@') {
		Some(i) => &identity[..path_start + i],
		None => identity,
	}
}

pub(crate) fn check(claims: &Claims, signer: &PackageSigner) -> Result<()> {
	match (claims, signer) {
		(
			Claims::Sigstore {
				issuer,
				identity,
				repository_id,
				owner_id,
				..
			},
			PackageSigner::Sigstore(expected),
		) => check_sigstore(issuer, identity, repository_id, owner_id, expected),
		(
			Claims::Key {
				key,
			},
			PackageSigner::Ssh(expected),
		) => {
			// A key written by hand may still carry its comment.
			let key = SshSigner::normalise(key).map_err(anyhow::Error::msg)?;
			let named = SshSigner::normalise(&expected.public_key).map_err(anyhow::Error::msg)?;
			if key != named {
				bail!("The signing key is {key}, but [signer.ssh] names {named}");
			}
			Ok(())
		}
		(
			Claims::Sigstore {
				..
			},
			PackageSigner::Ssh(_),
		) => {
			bail!("The signature is a Sigstore certificate, but [signer] names an SSH key")
		}
		(
			Claims::Key {
				..
			},
			PackageSigner::Sigstore(_),
		) => {
			bail!("The signature is an SSH signature, but [signer] names a Sigstore identity")
		}
	}
}

fn check_sigstore(
	issuer: &str,
	identity: &str,
	repository_id: &Option<String>,
	owner_id: &Option<String>,
	expected: &SigstoreSigner,
) -> Result<()> {
	if issuer != expected.issuer {
		bail!(
			"The certificate's issuer is {issuer}, but [signer.sigstore] names {}",
			expected.issuer
		);
	}
	// Any ref of the named workflow: `…/release.yml@refs/tags/v1` matches
	// `…/release.yml`. An identity without a scheme, such as an email
	// address, is compared as it is.
	let workflow = identity_without_ref(identity);
	if workflow != expected.identity {
		bail!(
			"The certificate's identity is {identity}, but [signer.sigstore] names {}",
			expected.identity
		);
	}
	for (field, expected, found) in [
		("repository_id", &expected.repository_id, repository_id),
		("owner_id", &expected.owner_id, owner_id),
	] {
		if let Some(expected) = expected
			&& found.as_ref() != Some(expected)
		{
			bail!(
				"The certificate's {field} is {}, but [signer.sigstore] names {expected}",
				found.as_deref().unwrap_or("absent")
			);
		}
	}
	Ok(())
}

#[cfg(test)]
pub(super) mod tests {
	use super::*;

	pub fn github() -> SigstoreSigner {
		SigstoreSigner {
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: "https://github.com/acme/color/.github/workflows/release.yml".into(),
			repository_id: Some("812345678".into()),
			owner_id: Some("98765432".into()),
		}
	}

	fn claims(identity: &str, repository_id: &str, owner_id: &str) -> Claims {
		Claims::Sigstore {
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: identity.into(),
			repository_id: Some(repository_id.into()),
			owner_id: Some(owner_id.into()),
			anchors: None,
		}
	}

	const WORKFLOW: &str = "https://github.com/acme/color/.github/workflows/release.yml";

	#[test]
	fn any_ref_of_the_named_workflow_matches() {
		let signer = PackageSigner::Sigstore(github());
		for identity in [WORKFLOW.to_string(), format!("{WORKFLOW}@refs/tags/v1.2.0")] {
			check(&claims(&identity, "812345678", "98765432"), &signer).unwrap();
		}
	}

	#[test]
	fn another_workflow_issuer_or_id_is_refused() {
		let signer = PackageSigner::Sigstore(github());
		let other = "https://github.com/acme/color/.github/workflows/other.yml@refs/heads/main";
		let err = check(&claims(other, "812345678", "98765432"), &signer).unwrap_err();
		assert!(err.to_string().contains("identity"), "{err}");
		// A repository that was deleted and re-created under the same name has
		// new ids.
		let err = check(&claims(WORKFLOW, "1", "98765432"), &signer).unwrap_err();
		assert!(err.to_string().contains("repository_id"), "{err}");
		let err = check(&claims(WORKFLOW, "812345678", "1"), &signer).unwrap_err();
		assert!(err.to_string().contains("owner_id"), "{err}");
		let gitlab = Claims::Sigstore {
			issuer: "https://gitlab.com".into(),
			identity: WORKFLOW.into(),
			repository_id: None,
			owner_id: None,
			anchors: None,
		};
		assert!(check(&gitlab, &signer).unwrap_err().to_string().contains("issuer"));
	}

	#[test]
	fn a_ci_token_is_checked_before_a_certificate_is_requested() {
		let signer = PackageSigner::Sigstore(github());
		let token = |workflow: &str, repository_id: &str| {
			serde_json::json!({
				"iss": "https://token.actions.githubusercontent.com",
				"job_workflow_ref": workflow,
				"repository_id": repository_id,
				"repository_owner_id": "98765432",
			})
		};
		let release = "acme/color/.github/workflows/release.yml@refs/tags/v1.2.0";
		ensure_token_matches(&token(release, "812345678"), &signer).unwrap();
		let other = "acme/color/.github/workflows/ci.yml@refs/heads/main";
		assert!(ensure_token_matches(&token(other, "812345678"), &signer).is_err());
		assert!(ensure_token_matches(&token(release, "1"), &signer).is_err());

		let gitlab = PackageSigner::Sigstore(SigstoreSigner {
			issuer: "https://gitlab.com".into(),
			identity: "https://gitlab.com/group/proj//.gitlab-ci.yml".into(),
			repository_id: Some("4242".into()),
			owner_id: Some("77".into()),
		});
		let claims = serde_json::json!({
			"iss": "https://gitlab.com",
			"ci_config_ref_uri": "gitlab.com/group/proj//.gitlab-ci.yml@refs/heads/main",
			"project_id": "4242",
			"namespace_id": "77",
		});
		ensure_token_matches(&claims, &gitlab).unwrap();
		assert!(ensure_token_matches(&claims, &signer).is_err());

		let ssh = PackageSigner::Ssh(SshSigner {
			public_key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA==".into(),
		});
		assert!(ensure_token_matches(&claims, &ssh).unwrap_err().to_string().contains("--key"));
	}

	#[test]
	fn an_email_identity_is_compared_as_written() {
		let signer = PackageSigner::Sigstore(SigstoreSigner {
			issuer: "https://github.com/login/oauth".into(),
			identity: "release@acme.example".into(),
			repository_id: None,
			owner_id: None,
		});
		assert!(signer.validate().is_ok());
		let claims = |identity: &str| Claims::Sigstore {
			issuer: "https://github.com/login/oauth".into(),
			identity: identity.into(),
			repository_id: None,
			owner_id: None,
			anchors: None,
		};
		check(&claims("release@acme.example"), &signer).unwrap();
		assert!(check(&claims("release@other.example"), &signer).is_err());
	}

	#[test]
	fn the_ssh_key_must_be_the_named_one() {
		let signer = PackageSigner::Ssh(SshSigner {
			public_key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA==".into(),
		});
		let mine = Claims::Key {
			key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA==".into(),
		};
		check(&mine, &signer).unwrap();
		let commented = PackageSigner::Ssh(SshSigner {
			public_key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA== me@host".into(),
		});
		check(&mine, &commented).unwrap();
		let theirs = Claims::Key {
			key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIB==".into(),
		};
		assert!(check(&theirs, &signer).is_err());
		let sigstore = claims(WORKFLOW, "812345678", "98765432");
		assert!(check(&sigstore, &signer).unwrap_err().to_string().contains("SSH key"));
	}
}
