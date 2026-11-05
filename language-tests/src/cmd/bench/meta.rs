//! Metadata describing the conditions a benchmark run was measured under,
//! embedded into every stored measurement so uploaded results are easy to
//! compare across transports, commits and machines.
//!
//! The `transport` field also keys the baseline (see `store/schema.surql`), so a
//! run over `http`/`ws` is never compared against an `embedded` baseline.
//!
//! Captured once per `bench run` at runtime (not at build time): the `profiling`
//! binary is reused across many commits/branches by the bench scripts, so a
//! build-time capture would record the wrong revision.

use std::process::Command;

use surrealdb_types::SurrealValue;

#[derive(Clone, SurrealValue)]
pub struct RunMetadata {
	/// Where the benchmark statement executed: `embedded` | `http` | `ws`.
	pub transport: String,
	/// The remote endpoint URL, when not embedded.
	pub endpoint: Option<String>,
	/// Full git commit SHA of the working tree the run was measured against.
	pub commit: Option<String>,
	/// Abbreviated commit SHA.
	pub commit_short: Option<String>,
	/// Current git branch.
	pub branch: Option<String>,
	/// Whether the working tree had uncommitted changes when the run started.
	pub dirty: Option<bool>,
	/// The machine the run was measured on.
	pub hostname: Option<String>,
	/// `std::env::consts::OS` (e.g. `macos`, `linux`).
	pub os: String,
	/// `std::env::consts::ARCH` (e.g. `aarch64`, `x86_64`).
	pub arch: String,
	/// CPU model string.
	pub cpu: Option<String>,
	/// Logical CPU count.
	pub cpu_cores: Option<i64>,
}

impl RunMetadata {
	/// Gather metadata for a run executed over the given `transport`
	/// (`embedded` | `http` | `ws`), with the remote `endpoint` URL when remote.
	pub fn gather(transport: &str, endpoint: Option<String>) -> RunMetadata {
		let (commit, commit_short, branch, dirty) = git_info();
		let (hostname, cpu, cpu_cores) = host_info();
		RunMetadata {
			transport: transport.to_string(),
			endpoint,
			commit,
			commit_short,
			branch,
			dirty,
			hostname,
			os: std::env::consts::OS.to_string(),
			arch: std::env::consts::ARCH.to_string(),
			cpu,
			cpu_cores,
		}
	}
}

/// Runs `git <args>` and returns trimmed stdout, or `None` if git is missing,
/// fails, or prints nothing (best-effort: a non-git checkout doesn't break benching).
fn git_output(args: &[&str]) -> Option<String> {
	let out = Command::new("git").args(args).output().ok()?;
	if !out.status.success() {
		return None;
	}
	let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
	(!s.is_empty()).then_some(s)
}

/// The commit a run is attributable to.
///
/// `GITHUB_SHA` wins where it is set. A CI job can bench a binary built
/// elsewhere, from a container or artifact whose working tree is not the
/// checkout — or not a git repository at all — and the revision the workflow ran
/// is the one a stored measurement has to be filed under. Falls back to the
/// working tree's `HEAD`.
pub fn current_commit() -> Option<String> {
	if let Ok(sha) = std::env::var("GITHUB_SHA")
		&& !sha.is_empty()
	{
		return Some(sha);
	}
	git_output(&["rev-parse", "HEAD"])
}

/// The branch a run is attributable to.
///
/// CI checks out a commit, not a branch — the PR benchmark asks for the head
/// SHA explicitly — and on a detached `HEAD` git answers the literal string
/// `HEAD`, which would file every CI measurement under the same meaningless
/// branch. GitHub names the branch in the environment instead: `GITHUB_HEAD_REF`
/// is the source branch of a pull request, and `GITHUB_REF_NAME` the branch or
/// tag of every other event. Falls back to the working tree, where a detached
/// `HEAD` is reported as no branch rather than as one called `HEAD`.
pub fn current_branch() -> Option<String> {
	for key in ["GITHUB_HEAD_REF", "GITHUB_REF_NAME"] {
		if let Ok(name) = std::env::var(key)
			&& !name.is_empty()
		{
			return Some(name);
		}
	}

	git_output(&["rev-parse", "--abbrev-ref", "HEAD"]).filter(|branch| branch != "HEAD")
}

fn git_info() -> (Option<String>, Option<String>, Option<String>, Option<bool>) {
	let commit = current_commit();
	// Abbreviate the commit above rather than asking git a second question:
	// git's answer describes the working tree, which is not always what the run
	// is attributable to. Its own abbreviation is preferred when the two agree,
	// since the length is repo-dependent.
	let commit_short = git_output(&["rev-parse", "--short", "HEAD"])
		.filter(|short| commit.as_deref().is_some_and(|c| c.starts_with(short.as_str())))
		.or_else(|| commit.as_deref().map(|c| c[..c.len().min(9)].to_string()));
	let branch = current_branch();
	let dirty = Command::new("git")
		.args(["status", "--porcelain"])
		.output()
		.ok()
		.filter(|o| o.status.success())
		.map(|o| !o.stdout.is_empty());
	(commit, commit_short, branch, dirty)
}

fn host_info() -> (Option<String>, Option<String>, Option<i64>) {
	let hostname = sysinfo::System::host_name();
	let mut sys = sysinfo::System::new();
	sys.refresh_cpu_all();
	let cpu = sys.cpus().first().map(|c| c.brand().trim().to_string()).filter(|s| !s.is_empty());
	let cpu_cores = Some(num_cpus::get() as i64);
	(hostname, cpu, cpu_cores)
}
