use crate::cli::version_client;
use crate::cli::version_client::VersionClient;
use crate::cnf::PKG_VERSION;
use crate::err::Error;
use clap::Args;
use semver::{Comparator, Op, Version};
use std::borrow::Cow;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::ops::Deref;
use std::path::Path;
use std::process::Command;
use surrealdb::env::{arch, os};

pub(crate) const ROOT: &str = "https://download.surrealdb.com";
const ALPHA: &str = "alpha";
const BETA: &str = "beta";
const LATEST: &str = "latest";
const NIGHTLY: &str = "nightly";

#[derive(Args, Debug)]
pub struct UpgradeCommandArguments {
	/// Install the latest nightly version
	#[arg(long, conflicts_with = "alpha", conflicts_with = "beta", conflicts_with = "version")]
	nightly: bool,
	/// Install the latest alpha version
	#[arg(long, conflicts_with = "nightly", conflicts_with = "beta", conflicts_with = "version")]
	alpha: bool,
	/// Install the latest beta version
	#[arg(long, conflicts_with = "nightly", conflicts_with = "alpha", conflicts_with = "version")]
	beta: bool,
	/// Install a specific version
	#[arg(long, conflicts_with = "nightly", conflicts_with = "alpha", conflicts_with = "beta")]
	version: Option<String>,
	/// Don't actually replace the executable
	#[arg(long)]
	dry_run: bool,
}

impl UpgradeCommandArguments {
	/// Get the version string to download based on the user preference
	async fn version(&self) -> Result<Cow<'_, str>, Error> {
		// Convert the version to lowercase, if supplied
		let version = self.version.as_deref().map(str::to_ascii_lowercase);
		let client = version_client::new(None)?;

		if self.nightly || version.as_deref() == Some(NIGHTLY) {
			Ok(Cow::Borrowed(NIGHTLY))
		} else if self.alpha || version.as_deref() == Some(ALPHA) {
			client.fetch(ALPHA).await
		} else if self.beta || version.as_deref() == Some(BETA) {
			client.fetch(BETA).await
		} else if let Some(version) = version {
			// Parse the version string to make sure it's valid, return an error if not
			let version = parse_version(&version)?;
			// Return the version, ensuring it's prefixed by `v`
			Ok(Cow::Owned(format!("v{version}")))
		} else {
			client.fetch(LATEST).await
		}
	}
}

pub(crate) fn parse_version(input: &str) -> Result<Version, Error> {
	// Remove the `v` prefix, if supplied
	let version = input.strip_prefix('v').unwrap_or(input);
	// Parse the version
	let comp = Comparator::parse(version)
		.map_err(|_| Error::Other(format!("Invalid version `{input}`",)))?;
	// See if a supported operation was requested
	if !matches!(comp.op, Op::Exact | Op::Caret) {
		return Err(Error::Other(format!(
			"Unsupported version `{version}`. Only exact matches are supported."
		)));
	}
	// Build and return the version if supported
	match (comp.minor, comp.patch) {
		(Some(minor), Some(patch)) => {
			let mut version = Version::new(comp.major, minor, patch);
			version.pre = comp.pre;
			Ok(version)
		}
		_ => Err(Error::Other(format!(
			"Unsupported version `{version}`. Please specify a full version, like `v1.2.1`."
		))),
	}
}

pub async fn init(args: UpgradeCommandArguments) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	let (outg, errg) = crate::telemetry::builder().with_log_level("error").init()?;

	// Upgrading overwrites the existing executable
	let exe = std::env::current_exe()?;

	// Check if we have write permissions
	let metadata = fs::metadata(&exe)?;
	let permissions = metadata.permissions();
	if permissions.readonly() {
		return Err(Error::Io(IoError::new(
			ErrorKind::PermissionDenied,
			"executable is read-only",
		)));
	}
	#[cfg(unix)]
	if std::os::unix::fs::MetadataExt::uid(&metadata) == 0
		&& !nix::unistd::Uid::effective().is_root()
	{
		return Err(Error::Io(IoError::new(
			ErrorKind::PermissionDenied,
			"executable is owned by root; try again with sudo",
		)));
	}

	// Compare old and new versions
	let old_version = PKG_VERSION.deref().clone();
	let new_version = args.version().await?;

	// Parsed version numbers follow semver format (major.minor.patch)
	if new_version != NIGHTLY && new_version != ALPHA && new_version != BETA {
		let old_version_parsed = parse_version(&old_version)?;
		let new_version_parsed = parse_version(&new_version)?;

		if old_version_parsed == new_version_parsed {
			println!("{old_version} is already installed");
			return Ok(());
		}
	}

	let arch = arch();
	let os = os();

	println!("current version is {old_version} for {os} on {arch}",);

	let download_arch = match arch {
		"aarch64" => "arm64",
		"x86_64" => "amd64",
		_ => {
			return Err(Error::Io(IoError::new(
				ErrorKind::Unsupported,
				format!("unsupported arch {arch}"),
			)));
		}
	};

	let (download_os, download_ext) = match os {
		"linux" => ("linux", "tgz"),
		"macos" => ("darwin", "tgz"),
		"windows" => ("windows", "exe"),
		_ => {
			return Err(Error::Io(IoError::new(
				ErrorKind::Unsupported,
				format!("unsupported OS {os}"),
			)));
		}
	};

	println!("downloading {new_version} for {download_os} on {download_arch}");

	let download_filename =
		format!("surreal-{new_version}.{download_os}-{download_arch}.{download_ext}");
	let url = format!("{ROOT}/{new_version}/{download_filename}");

	let response = reqwest::get(&url).await?;

	if !response.status().is_success() {
		return Err(Error::Io(IoError::new(
			ErrorKind::Other,
			format!("received status {} when downloading from {url}", response.status()),
		)));
	}

	let binary = response.bytes().await?;

	// Create a temporary file path
	let tmp_dir = tempfile::tempdir()?;
	let mut tmp_path = tmp_dir.path().join(download_filename);

	// Download to a temp file to avoid writing to a running exe file
	fs::write(&tmp_path, &*binary)?;

	// Preserve permissions
	fs::set_permissions(&tmp_path, permissions)?;

	// Unarchive
	if download_ext == "tgz" {
		let output = Command::new("tar")
			.arg("-zxf")
			.arg(&tmp_path)
			.arg("-C")
			.arg(tmp_dir.path())
			.output()?;
		if !output.status.success() {
			return Err(Error::Io(IoError::new(
				ErrorKind::Other,
				format!("failed to unarchive: {}", output.status),
			)));
		}

		// focus on the extracted path
		tmp_path = tmp_dir.path().join("surreal");
	}

	println!("installing at {}", exe.display());

	// Replace the running executable
	if args.dry_run {
		println!("Dry run successfully completed")
	} else {
		replace_exe(&tmp_path, &exe)?;
		println!("SurrealDB successfully upgraded");
	}

	// Drop the log guards
	drop(outg);
	drop(errg);
	// Everything OK
	Ok(())
}

/// Replace exe at `to` with contents of `from`
fn replace_exe(from: &Path, to: &Path) -> Result<(), IoError> {
	if cfg!(windows) {
		fs::rename(to, to.with_extension("old.exe"))?;
	} else {
		fs::remove_file(to)?;
	}
	// Rename works when from and to are on the same file system/device, but
	// fall back to copy if they're not
	fs::rename(from, to).or_else(|_| {
		// Don't worry about deleting the file as the tmp directory will
		// be deleted automatically
		fs::copy(from, to).map(|_| ())
	})
}
