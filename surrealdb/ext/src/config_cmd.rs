//! `surreal ext config`: read and change a project's `package.toml`.
//!
//! Every change goes through [`PackageConfig`], so it is validated before it
//! is written, and comments and formatting of everything it does not change
//! survive.
//!
//! ```text
//! surreal ext config show
//! surreal ext config get package.version
//! surreal ext config set capabilities.allow_write true
//! surreal ext config add capabilities.allow_net api.color.io:443
//! surreal ext config signer github --repo acme/color --workflow release.yml
//! surreal ext config fs add ../models --as /models
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use surrealism_runtime::config::{PackageSigner, SigstoreSigner, SshSigner};
use surrealism_runtime::package_config::PackageConfig;

/// Arguments of `surreal ext config`.
#[derive(Debug, Args)]
pub struct ConfigArgs {
	/// The project directory (defaults to the current directory)
	#[arg(long, value_name = "PATH", global = true)]
	path: Option<PathBuf>,

	#[command(subcommand)]
	command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
	/// Print the package file
	Show,
	/// Print one value, such as `package.version` or `capabilities.allow_net`
	Get {
		key: String,
	},
	/// Set one value
	Set {
		key: String,
		value: String,
	},
	/// Add a value to a list: `capabilities.allow_net` or `capabilities.allow_functions`
	Add {
		key: String,
		value: String,
	},
	/// Remove a value from a list: `capabilities.allow_net` or `capabilities.allow_functions`
	Remove {
		key: String,
		value: String,
	},
	/// Name the one signer the package's archives may be signed by
	#[command(subcommand)]
	Signer(SignerCommand),
	/// Bundle directories into the archive's read-only filesystem
	#[command(subcommand)]
	Fs(FsCommand),
}

#[derive(Debug, Subcommand)]
enum SignerCommand {
	/// Sigstore keyless signing from a GitHub Actions workflow
	Github {
		/// The repository, as `owner/name`
		#[arg(long)]
		repo: String,
		/// The workflow file, such as `release.yml`
		#[arg(long)]
		workflow: String,
		/// The repository's numeric id; read from the GitHub API when omitted
		#[arg(long, requires = "owner_id")]
		repository_id: Option<String>,
		/// The owner's numeric id; read from the GitHub API when omitted
		#[arg(long, requires = "repository_id")]
		owner_id: Option<String>,
		#[arg(long, hide = true, default_value = "https://api.github.com")]
		api_url: String,
	},
	/// Sigstore keyless signing from GitLab CI
	Gitlab {
		/// The project path, such as `group/project`
		#[arg(long)]
		project: String,
		/// The GitLab instance
		#[arg(long, default_value = "https://gitlab.com")]
		host: String,
		/// The CI configuration file the pipeline runs
		#[arg(long, default_value = ".gitlab-ci.yml")]
		ci_config: String,
		/// The project's numeric id; read from the GitLab API when omitted
		#[arg(long, requires = "namespace_id")]
		project_id: Option<String>,
		/// The project namespace's numeric id; read from the GitLab API when omitted
		#[arg(long, requires = "project_id")]
		namespace_id: Option<String>,
		/// The API to read the ids from, when it is not `{host}/api/v4`
		#[arg(long, hide = true)]
		api_url: Option<String>,
	},
	/// Signing with an SSH key
	Ssh {
		/// The public key file, such as `~/.ssh/release.pub`
		#[arg(value_name = "FILE")]
		public_key: PathBuf,
	},
	/// Remove [signer]
	Remove,
}

#[derive(Debug, Subcommand)]
enum FsCommand {
	/// Mount a local directory, relative to the project or absolute
	Add {
		#[arg(value_name = "DIR")]
		dir: String,
		/// Where the module sees it
		#[arg(long = "as", value_name = "PATH", default_value = "/")]
		mount: String,
	},
	/// Remove the mount at a path
	Remove {
		#[arg(value_name = "PATH")]
		mount: String,
	},
}

pub async fn init(args: ConfigArgs) -> Result<()> {
	let dir = match args.path {
		Some(dir) => dir,
		None => std::env::current_dir().context("Failed to determine current directory")?,
	};
	let output = run(&dir, args.command).await?;
	print!("{output}");
	Ok(())
}

/// Run `command` on the project in `dir`, and return what to print.
async fn run(dir: &Path, command: ConfigCommand) -> Result<String> {
	let mut package = PackageConfig::open(dir)?;
	let changed = match command {
		ConfigCommand::Show => return Ok(package.render()?),
		ConfigCommand::Get {
			key,
		} => return get(&package, &key),
		ConfigCommand::Set {
			key,
			value,
		} => {
			set(&mut package, &key, &value)?;
			format!("Set {key} = {value}")
		}
		ConfigCommand::Add {
			key,
			value,
		} => match add(&mut package, &key, &value)? {
			true => format!("Added {value} to {key}"),
			false => format!("{key} already holds {value}"),
		},
		ConfigCommand::Remove {
			key,
			value,
		} => match remove(&mut package, &key, &value)? {
			true => format!("Removed {value} from {key}"),
			false => format!("{key} does not hold {value}"),
		},
		ConfigCommand::Signer(signer) => {
			let signer = match signer {
				SignerCommand::Remove => None,
				signer @ (SignerCommand::Github {
					..
				}
				| SignerCommand::Gitlab {
					..
				}
				| SignerCommand::Ssh {
					..
				}) => Some(signer_from(signer).await?),
			};
			let described = match &signer {
				Some(PackageSigner::Sigstore(s)) => {
					format!("Signer: {} from {}", s.identity, s.issuer)
				}
				Some(PackageSigner::Ssh(s)) => format!("Signer: {}", s.public_key),
				None => "Removed [signer]".to_string(),
			};
			package.set_signer(signer)?;
			described
		}
		ConfigCommand::Fs(FsCommand::Add {
			dir: local,
			mount,
		}) => {
			package.add_fs(&local, &mount)?;
			format!("Mounted {local} at {mount}")
		}
		ConfigCommand::Fs(FsCommand::Remove {
			mount,
		}) => match package.remove_fs(&mount)? {
			true => format!("Removed the mount at {mount}"),
			false => format!("Nothing is mounted at {mount}"),
		},
	};
	package.save()?;
	let path =
		package.path().map_or_else(|| "the package file".into(), |p| p.display().to_string());
	Ok(format!("{changed} in {path}\n"))
}

/// The value at the dotted `key` of the package file, defaults included.
fn get(package: &PackageConfig, key: &str) -> Result<String> {
	match package.get(key)? {
		Some(value) if value.ends_with('\n') => Ok(value),
		Some(value) => Ok(format!("{value}\n")),
		None => bail!("{key} is not set"),
	}
}

const SETTABLE: &str = "package.name, package.organisation, package.version, \
	capabilities.allow_scripting, capabilities.allow_arbitrary_queries, capabilities.allow_write, \
	capabilities.strict_timeout, capabilities.max_memory_bytes, capabilities.max_execution_time";

fn set(package: &mut PackageConfig, key: &str, value: &str) -> Result<()> {
	let flag = || -> Result<bool> {
		value.parse().with_context(|| format!("{key} is `true` or `false`, not {value:?}"))
	};
	let limit = || -> Result<Option<u64>> {
		match value {
			"none" => Ok(None),
			n => n.parse().map(Some).with_context(|| format!("{key} is a number or `none`")),
		}
	};
	match key {
		"package.name" => package.set_name(value)?,
		"package.organisation" => package.set_organisation(value)?,
		"package.version" => package.set_version(value)?,
		"capabilities.allow_scripting" => package.capabilities_mut().set_scripting(flag()?),
		"capabilities.allow_arbitrary_queries" => package.capabilities_mut().set_queries(flag()?),
		"capabilities.allow_write" => package.capabilities_mut().set_write(flag()?),
		"capabilities.strict_timeout" => package.capabilities_mut().set_strict_timeout(flag()?),
		"capabilities.max_memory_bytes" => {
			let bytes = limit()?.map(usize::try_from).transpose().context("too large")?;
			package.capabilities_mut().set_max_memory(bytes)?;
		}
		"capabilities.max_execution_time" => {
			package
				.capabilities_mut()
				.set_max_execution_time(limit()?.map(Duration::from_millis))?;
		}
		"capabilities.allow_net" | "capabilities.allow_functions" => {
			bail!("{key} is a list; use `add` and `remove`")
		}
		_ => bail!("{key} cannot be set; settable keys are {SETTABLE}"),
	}
	Ok(())
}

fn add(package: &mut PackageConfig, key: &str, value: &str) -> Result<bool> {
	Ok(match key {
		"capabilities.allow_net" => package.capabilities_mut().add_net(value)?,
		"capabilities.allow_functions" => package.capabilities_mut().add_function(value)?,
		_ => bail!(
			"{key} is not a list; the lists are capabilities.allow_net and capabilities.allow_functions"
		),
	})
}

fn remove(package: &mut PackageConfig, key: &str, value: &str) -> Result<bool> {
	Ok(match key {
		"capabilities.allow_net" => package.capabilities_mut().remove_net(value),
		"capabilities.allow_functions" => package.capabilities_mut().remove_function(value),
		_ => bail!(
			"{key} is not a list; the lists are capabilities.allow_net and capabilities.allow_functions"
		),
	})
}

/// The signer a `signer` subcommand names, reading ids from the forge's API
/// when they are not given.
async fn signer_from(command: SignerCommand) -> Result<PackageSigner> {
	Ok(match command {
		SignerCommand::Github {
			repo,
			workflow,
			repository_id,
			owner_id,
			api_url,
		} => {
			let repo = project_path(&repo, "--repo")?;
			if repo.split('/').count() != 2 {
				bail!("--repo is `owner/name`, not {repo:?}");
			}
			let workflow = workflow.rsplit('/').next().unwrap_or(&workflow).to_string();
			if !(workflow.ends_with(".yml") || workflow.ends_with(".yaml"))
				|| workflow.contains('@')
			{
				bail!("--workflow is the workflow file, such as `release.yml`");
			}
			// The API's `full_name` is the repository's current, canonically
			// cased path, which is what Fulcio writes into the certificate.
			let (repo, repository_id, owner_id) = match (repository_id, owner_id) {
				(Some(repository), Some(owner)) => (repo, repository, owner),
				_ => {
					let url = format!("{}/repos/{repo}", api_url.trim_end_matches('/'));
					let token = std::env::var("GITHUB_TOKEN").ok().filter(|t| !t.is_empty());
					let body =
						fetch_json(&url, token.map(|t| ("Authorization", format!("Bearer {t}"))))
							.await?;
					let canonical = text(&body["full_name"], &url)?;
					let repo = project_path(&canonical, "full_name")?;
					(repo, numeric(&body["id"], &url)?, numeric(&body["owner"]["id"], &url)?)
				}
			};
			PackageSigner::Sigstore(SigstoreSigner {
				issuer: "https://token.actions.githubusercontent.com".into(),
				identity: format!("https://github.com/{repo}/.github/workflows/{workflow}"),
				repository_id: Some(repository_id),
				owner_id: Some(owner_id),
			})
		}
		SignerCommand::Gitlab {
			project,
			host,
			ci_config,
			project_id,
			namespace_id,
			api_url,
		} => {
			let project = project_path(&project, "--project")?;
			let host = host.trim_end_matches('/').to_string();
			let api = api_url.map_or_else(
				|| format!("{host}/api/v4"),
				|api| api.trim_end_matches('/').to_string(),
			);
			if !host.starts_with("https://") {
				bail!("--host {host:?} is not an https URL; it is the issuer of the certificate");
			}
			// `path_with_namespace` is the project's current, canonically cased
			// path, which is what GitLab writes into the identity token.
			let (project, project_id, namespace_id) = match (project_id, namespace_id) {
				(Some(id), Some(namespace)) => (project, id, namespace),
				_ => {
					let url = format!("{api}/projects/{}", project.replace('/', "%2F"));
					let token = std::env::var("GITLAB_TOKEN").ok().filter(|t| !t.is_empty());
					let body = fetch_json(&url, token.map(|t| ("PRIVATE-TOKEN", t))).await?;
					let canonical = text(&body["path_with_namespace"], &url)?;
					let project = project_path(&canonical, "path_with_namespace")?;
					(project, numeric(&body["id"], &url)?, numeric(&body["namespace"]["id"], &url)?)
				}
			};
			PackageSigner::Sigstore(SigstoreSigner {
				issuer: host.clone(),
				identity: format!("{host}/{project}//{}", ci_config.trim_start_matches('/')),
				repository_id: Some(project_id),
				owner_id: Some(namespace_id),
			})
		}
		SignerCommand::Ssh {
			public_key,
		} => {
			let text = std::fs::read_to_string(&public_key)
				.with_context(|| format!("Failed to read {}", public_key.display()))?;
			PackageSigner::Ssh(SshSigner {
				public_key: SshSigner::normalise(&text).map_err(anyhow::Error::msg)?,
			})
		}
		SignerCommand::Remove => bail!("`signer remove` names no signer"),
	})
}

/// `path` if it is a forge path such as `group/sub/project`.
fn project_path(path: &str, flag: &str) -> Result<String> {
	let path = path.trim_matches('/');
	let segment = |s: &str| {
		!s.is_empty()
			&& s != "."
			&& s != ".."
			&& s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
	};
	if !path.contains('/') || !path.split('/').all(segment) {
		bail!("{flag} {path:?} is not a repository path such as `owner/name`");
	}
	Ok(path.to_string())
}

/// GET `url` as JSON, sending `auth` as a header.
///
/// A token goes only to an https URL: plain http is accepted for a loopback
/// address, without the token, and refused anywhere else. Redirects are not
/// followed, so a token cannot be carried to another host.
async fn fetch_json(url: &str, auth: Option<(&str, String)>) -> Result<serde_json::Value> {
	let parsed = reqwest::Url::parse(url).with_context(|| format!("{url} is not a URL"))?;
	let loopback = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
	let auth = match parsed.scheme() {
		"https" => auth,
		"http" if loopback => None,
		_ => bail!("{url} is not an https URL, so no request is sent to it"),
	};
	let http = reqwest::Client::builder()
		.timeout(Duration::from_secs(30))
		.user_agent(concat!("surreal/", env!("CARGO_PKG_VERSION")))
		.redirect(reqwest::redirect::Policy::none())
		.build()
		.context("Failed to build the HTTP client")?;
	let mut request = http.get(parsed).header("Accept", "application/json");
	if let Some((header, value)) = auth {
		request = request.header(header, value);
	}
	let response = request.send().await.with_context(|| format!("Failed to reach {url}"))?;
	let status = response.status();
	if status.is_redirection() {
		bail!("{url} answered {status}; redirects are not followed, give the final URL");
	}
	if !status.is_success() {
		bail!(
			"{url} answered {status}; for a private repository set GITHUB_TOKEN or GITLAB_TOKEN, \
			 or give the ids with --repository-id/--owner-id or --project-id/--namespace-id"
		);
	}
	response.json().await.with_context(|| format!("{url} did not answer JSON"))
}

/// The string at `value`.
fn text(value: &serde_json::Value, url: &str) -> Result<String> {
	value.as_str().map(str::to_owned).with_context(|| format!("{url} did not name the repository"))
}

/// The numeric id at `value`, as text.
fn numeric(value: &serde_json::Value, url: &str) -> Result<String> {
	value.as_u64().map(|id| id.to_string()).with_context(|| format!("{url} has no numeric id"))
}

#[cfg(test)]
mod tests {
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	use super::*;

	const PACKAGE: &str =
		"# Colour.\n[package]\norganisation = \"acme\"\nname = \"color\"\nversion = \"1.0.0\"\n";

	fn project() -> tempfile::TempDir {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(dir.path().join("package.toml"), PACKAGE).unwrap();
		dir
	}

	fn opened(dir: &tempfile::TempDir) -> PackageConfig {
		PackageConfig::open(dir.path()).unwrap()
	}

	fn sigstore(dir: &tempfile::TempDir) -> SigstoreSigner {
		match opened(dir).config().signer.clone() {
			Some(PackageSigner::Sigstore(s)) => s,
			other => panic!("{other:?}"),
		}
	}

	#[tokio::test]
	async fn set_get_add_and_remove_go_through_the_package_file() {
		let dir = project();
		let cmd = |key: &str, value: &str| ConfigCommand::Set {
			key: key.into(),
			value: value.into(),
		};
		run(dir.path(), cmd("package.version", "1.1.0")).await.unwrap();
		run(dir.path(), cmd("capabilities.allow_write", "true")).await.unwrap();
		run(dir.path(), cmd("capabilities.max_execution_time", "5000")).await.unwrap();
		run(
			dir.path(),
			ConfigCommand::Add {
				key: "capabilities.allow_net".into(),
				value: "api.color.io:443".into(),
			},
		)
		.await
		.unwrap();
		let get = |key: &str| {
			run(
				dir.path(),
				ConfigCommand::Get {
					key: key.into(),
				},
			)
		};
		assert_eq!(get("package.version").await.unwrap(), "1.1.0\n");
		assert_eq!(get("capabilities.allow_write").await.unwrap(), "true\n");
		assert_eq!(get("capabilities.max_execution_time").await.unwrap(), "5000\n");
		assert_eq!(get("capabilities.allow_net").await.unwrap(), "[\"api.color.io:443\"]\n");
		// A default is readable although the file never names it.
		assert_eq!(get("capabilities.strict_timeout").await.unwrap(), "true\n");
		assert!(get("capabilities.nope").await.is_err());

		run(
			dir.path(),
			ConfigCommand::Remove {
				key: "capabilities.allow_net".into(),
				value: "api.color.io:443".into(),
			},
		)
		.await
		.unwrap();
		assert_eq!(get("capabilities.allow_net").await.unwrap(), "[]\n");
		assert!(
			std::fs::read_to_string(dir.path().join("package.toml"))
				.unwrap()
				.starts_with("# Colour.\n")
		);

		assert!(run(dir.path(), cmd("package.version", "one")).await.is_err());
		assert!(run(dir.path(), cmd("capabilities.allow_net", "x")).await.is_err());
		assert!(run(dir.path(), cmd("abi", "3")).await.is_err());
	}

	#[tokio::test]
	async fn the_github_signer_reads_its_ids_from_the_api() {
		let api = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/repos/acme/color"))
			.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
				"id": 812345678u64,
				"full_name": "Acme/Color",
				"owner": { "id": 98765432u64, "login": "Acme" },
			})))
			.expect(1)
			.mount(&api)
			.await;
		let dir = project();
		run(
			dir.path(),
			ConfigCommand::Signer(SignerCommand::Github {
				repo: "acme/color".into(),
				workflow: ".github/workflows/release.yml".into(),
				repository_id: None,
				owner_id: None,
				api_url: api.uri(),
			}),
		)
		.await
		.unwrap();
		assert_eq!(
			sigstore(&dir),
			SigstoreSigner {
				issuer: "https://token.actions.githubusercontent.com".into(),
				identity: "https://github.com/Acme/Color/.github/workflows/release.yml".into(),
				repository_id: Some("812345678".into()),
				owner_id: Some("98765432".into()),
			}
		);
	}

	#[tokio::test]
	async fn the_github_signer_works_offline_with_the_ids_given() {
		let dir = project();
		run(
			dir.path(),
			ConfigCommand::Signer(SignerCommand::Github {
				repo: "acme/color".into(),
				workflow: "release.yml".into(),
				repository_id: Some("1".into()),
				owner_id: Some("2".into()),
				api_url: "http://127.0.0.1:9".into(),
			}),
		)
		.await
		.unwrap();
		let signer = sigstore(&dir);
		assert_eq!(signer.repository_id.as_deref(), Some("1"));
		assert_eq!(signer.owner_id.as_deref(), Some("2"));
	}

	#[tokio::test]
	async fn the_gitlab_signer_reads_its_ids_from_the_api() {
		let api = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/api/v4/projects/group%2Fproj"))
			.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
				"id": 4242u64,
				"path_with_namespace": "group/proj",
				"namespace": { "id": 77u64 },
			})))
			.expect(1)
			.mount(&api)
			.await;
		let dir = project();
		run(
			dir.path(),
			ConfigCommand::Signer(SignerCommand::Gitlab {
				project: "group/proj".into(),
				host: "https://gitlab.example.com".into(),
				ci_config: ".gitlab-ci.yml".into(),
				project_id: None,
				namespace_id: None,
				api_url: Some(format!("{}/api/v4", api.uri())),
			}),
		)
		.await
		.unwrap();
		let signer = sigstore(&dir);
		assert_eq!(signer.issuer, "https://gitlab.example.com");
		assert_eq!(signer.identity, "https://gitlab.example.com/group/proj//.gitlab-ci.yml");
		assert_eq!(signer.repository_id.as_deref(), Some("4242"));
		assert_eq!(signer.owner_id.as_deref(), Some("77"));
	}

	/// A token is never sent where it could be read in transit or carried
	/// elsewhere: not over plain http to another host, and not across a
	/// redirect.
	#[tokio::test]
	async fn tokens_go_only_to_https_and_redirects_are_refused() {
		let err =
			fetch_json("http://api.github.example/repos/a/b", Some(("Authorization", "t".into())))
				.await
				.unwrap_err();
		assert!(err.to_string().contains("not an https URL"), "{err}");

		let api = MockServer::start().await;
		Mock::given(method("GET"))
			.respond_with(
				ResponseTemplate::new(302).insert_header("Location", "https://elsewhere.example/"),
			)
			.mount(&api)
			.await;
		let err = fetch_json(&format!("{}/repos/a/b", api.uri()), None).await.unwrap_err();
		assert!(err.to_string().contains("redirects are not followed"), "{err}");

		// Loopback http is for tests and mirrors; it never carries the token.
		let api = MockServer::start().await;
		Mock::given(method("GET"))
			.and(wiremock::matchers::header_exists("Authorization"))
			.respond_with(ResponseTemplate::new(500))
			.mount(&api)
			.await;
		Mock::given(method("GET"))
			.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
			.mount(&api)
			.await;
		fetch_json(&format!("{}/x", api.uri()), Some(("Authorization", "t".into()))).await.unwrap();
	}

	#[tokio::test]
	async fn a_failing_api_changes_nothing() {
		let api = MockServer::start().await;
		Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&api).await;
		let dir = project();
		let err = run(
			dir.path(),
			ConfigCommand::Signer(SignerCommand::Github {
				repo: "acme/private".into(),
				workflow: "release.yml".into(),
				repository_id: None,
				owner_id: None,
				api_url: api.uri(),
			}),
		)
		.await
		.unwrap_err();
		assert!(err.to_string().contains("404"), "{err}");
		assert_eq!(std::fs::read_to_string(dir.path().join("package.toml")).unwrap(), PACKAGE);
	}

	#[tokio::test]
	async fn the_ssh_signer_keeps_the_key_blob_only_and_can_be_removed() {
		let dir = project();
		let key = dir.path().join("release.pub");
		std::fs::write(&key, "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA== release@acme\n").unwrap();
		run(
			dir.path(),
			ConfigCommand::Signer(SignerCommand::Ssh {
				public_key: key,
			}),
		)
		.await
		.unwrap();
		assert_eq!(
			opened(&dir).config().signer,
			Some(PackageSigner::Ssh(SshSigner {
				public_key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA==".into(),
			}))
		);
		run(dir.path(), ConfigCommand::Signer(SignerCommand::Remove)).await.unwrap();
		assert_eq!(std::fs::read_to_string(dir.path().join("package.toml")).unwrap(), PACKAGE);
	}

	#[tokio::test]
	async fn fs_mounts_are_added_and_removed() {
		let dir = project();
		run(
			dir.path(),
			ConfigCommand::Fs(FsCommand::Add {
				dir: "../models".into(),
				mount: "/models".into(),
			}),
		)
		.await
		.unwrap();
		let mounts: Vec<(String, String)> = opened(&dir)
			.config()
			.attach
			.fs
			.as_ref()
			.unwrap()
			.iter()
			.map(|(g, l)| (g.to_string(), l.to_string()))
			.collect();
		assert_eq!(mounts, [("/models".to_string(), "../models".to_string())]);
		run(
			dir.path(),
			ConfigCommand::Fs(FsCommand::Remove {
				mount: "models".into(),
			}),
		)
		.await
		.unwrap();
		assert!(opened(&dir).config().attach.fs.is_none());
	}
}
