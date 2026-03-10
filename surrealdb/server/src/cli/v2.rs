use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use clap::Args;
use futures::{Stream, TryStreamExt};
use rand::distributions::DistString;
use semver::Version;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::cli::upgrade::file_platform_suffix;

#[derive(Args, Debug)]
pub struct V2Commands {
	#[arg(long, help = "Give the cli permission in advance to download new binaries if required")]
	accept: bool,
	#[arg(
		trailing_var_arg = true,
		allow_hyphen_values = true,
		help = "Commands to pass to the 2.0 binary"
	)]
	args: Vec<String>,
}

pub async fn update_modified_time(file: &Path) -> Result<()> {
	let f = tokio::fs::File::open(file).await?;
	f.into_std().await.set_modified(SystemTime::now())?;
	Ok(())
}

pub async fn get_existing_version(file: &Path) -> Option<Version> {
	let res = tokio::time::timeout(Duration::from_secs(1), async {
		tokio::process::Command::new(file).arg("version").kill_on_drop(true).output().await
	})
	.await;

	let res = res
		.inspect_err(|e| warn!("Could not run cached surrealdb v2 binary within timelimit: {e}"))
		.ok()?
		.inspect_err(|e| warn!("Failed to run cached surrealdb v2 binary: {e}"))
		.ok()?;
	let version = str::from_utf8(&res.stdout).ok()?;
	let version = version.split_whitespace().next()?.trim();

	let _ = update_modified_time(file)
		.await
		.inspect_err(|e| warn!("Could not update the modification of the file: {e}"));
	semver::Version::parse(version).ok()
}

pub async fn get_latest_version() -> Option<Version> {
	let body = reqwest::get("https://download.surrealdb.com/v2.txt")
		.await
		.inspect_err(|e| warn!("Could not fetch latest v2 version: {e}"))
		.ok()?
		.error_for_status()
		.inspect_err(|e| warn!("Could not fetch latest version from download page: {e}"))
		.ok()?;

	let body =
		body.text().await.inspect_err(|e| warn!("Error fetching latest v2 version :{e}")).ok()?;
	let body = body.trim();
	semver::Version::parse(body.strip_prefix("v").unwrap_or(body)).ok()
}

pub async fn ask_permission(source: &str, path: &Path) -> bool {
	print!(
		"Can I download the latest 2.0 surrealdb binary from '{source}' to '{}'? [y/n] ",
		path.display()
	);
	std::io::stdout().flush().expect("Flushing stdio to work");
	let buffer = BufReader::new(tokio::io::stdin());
	let mut lines = buffer.lines();
	while let Ok(Some(x)) = lines.next_line().await {
		match x.trim() {
			"y" | "Y" => return true,
			"n" | "N" => return false,
			_ => println!("Please enter either `y` or `n`"),
		}
	}
	false
}

async fn flush<S, W>(s: &mut S, w: &mut W) -> Result<()>
where
	S: Stream<Item = reqwest::Result<Bytes>> + Unpin,
	W: tokio::io::AsyncWriteExt + Unpin,
{
	while let Some(x) = s.try_next().await? {
		w.write_all(&x).await?;
	}
	Ok(())
}

#[cfg(not(target_family = "unix"))]
async fn make_executable(f: &mut File) -> Result<()> {
	Ok(())
}

#[cfg(target_family = "unix")]
async fn make_executable(f: &mut File) -> Result<()> {
	use std::os::unix::fs::PermissionsExt;
	let mut permissions = f.metadata().await?.permissions();
	permissions.set_mode(0o755);
	f.set_permissions(permissions).await?;
	Ok(())
}

pub async fn download_v2(
	has_permission: bool,
	path: &Path,
	version: &Version,
) -> Result<Option<PathBuf>> {
	let rand = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
	let temp_path = std::env::temp_dir().join(format!("surreal-{}", rand));

	let suffix = file_platform_suffix()?;
	let url = format!("https://download.surrealdb.com/v{version}/surreal-v{version}.{suffix}");

	if !has_permission && !ask_permission(&url, path).await {
		return Ok(None);
	}

	info!("Downloading v2 binary");

	let res =
		reqwest::get(url).await.context("Could not access surrealdb v2 binary download page")?;

	let res = res.error_for_status().context("Download page returned an error status code")?;

	let mut file = tokio::fs::OpenOptions::new()
		.create(true)
		.write(true)
		.truncate(true)
		.open(&temp_path)
		.await
		.with_context(|| {
			format!("Could not create temporary download file in '{}'", temp_path.display())
		})?;

	let mut reader = res.bytes_stream();
	flush(&mut reader, &mut file).await.context("Failed to download v2 binary")?;

	if suffix.ends_with("tgz") {
		let mut extraction_path = temp_path.clone();
		extraction_path.set_extension("bin");
		let mut file = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&extraction_path)
			.await
			.with_context(|| {
				format!(
					"Could not create temporary download file in '{}'",
					extraction_path.display()
				)
			})?;

		let mut output = tokio::process::Command::new("tar")
			.arg("-zxf")
			.arg(&temp_path)
			.arg("-O")
			.arg("surreal")
			.stdout(Stdio::piped())
			.spawn()
			.context("Could not run `tar` to extract downloaded file")?;

		let mut stdout = output.stdout.take().expect("Stdout should be present");

		let task = tokio::spawn(async move {
			tokio::io::copy(&mut stdout, &mut file).await.context("Failed to extract v2 binary")?;
			make_executable(&mut file).await.context("Failed to make v2 binary executable")
		});

		let out = output.wait().await.context("Failed to extract v2 binary")?;
		let statuscode = out.code().unwrap_or(-1);
		ensure!(out.success(), "Extraction process failed with statuscode: {statuscode}");
		task.await??;

		info!("Successfully downloaded v2 binary");

		Ok(Some(extraction_path))
	} else {
		make_executable(&mut file).await.context("Failed to make v2 binary executable")?;
		info!("Successfully downloaded v2 binary");
		Ok(Some(temp_path))
	}
}

pub async fn move_binary(from: &Path, to: &Path) -> std::io::Result<()> {
	match tokio::fs::rename(from, to).await {
		Ok(_) => Ok(()),
		Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
			tokio::fs::copy(from, to).await?;
			let _ = tokio::fs::remove_file(from).await;
			Ok(())
		}
		Err(e) => Err(e),
	}
}

pub async fn ensure_binary_present(has_permission: bool) -> Result<PathBuf> {
	let dir = dirs::cache_dir().unwrap_or_else(std::env::temp_dir).join("surrealdb");
	let file = dir.join("surreal_v2");

	if tokio::fs::try_exists(&file).await.context("Cannot access system cache directory")? {
		let meta = tokio::fs::metadata(&file)
			.await
			.context("Cannot read metadata of cached v2 surrealdb executable")?;
		let modified =
			meta.modified().context("Cannot read modified time of surrealdb executable")?;
		if modified.elapsed().map(|x| x > Duration::from_secs(24 * 60 * 60)).unwrap_or(true) {
			if let Some(exiting_version) = get_existing_version(&file).await {
				if let Some(latest_version) = get_latest_version().await
					&& exiting_version < latest_version
					&& let Some(tmp_path) =
						download_v2(has_permission, &file, &latest_version).await?
				{
					tokio::fs::remove_file(&file)
						.await
						.context("Cannot access cached surrealdb v2 binary")?;
					move_binary(&tmp_path, &file)
						.await
						.context("Could not move downloaded file to cache directory")?;
				}
				return Ok(file);
			} else {
				let Some(latest_version) = get_latest_version().await else {
					bail!(
						"Could not retrieve latest surrealdb version, cannot download nor run the v2 binary"
					);
				};
				let _ = tokio::fs::remove_file(&file).await;
				if let Some(tmp_path) = download_v2(has_permission, &file, &latest_version).await? {
					tokio::fs::rename(tmp_path, &file)
						.await
						.context("Could not rename downloaded binary")?;
				} else {
					bail!("Cannot run v2 binary")
				}
			}
		}
		Ok(file)
	} else {
		let Some(latest_version) = get_latest_version().await else {
			bail!(
				"Could not retrieve latest surrealdb version, cannot download nor run the v2 binary"
			);
		};
		if let Some(tmp_path) = download_v2(has_permission, &file, &latest_version).await? {
			tokio::fs::create_dir_all(dir)
				.await
				.context("Could not create surrealdb v2 binary cache directory")?;
			tokio::fs::rename(tmp_path, &file)
				.await
				.context("Could not rename downloaded binary")?;
			Ok(file)
		} else {
			bail!("Cannot run v2 binary")
		}
	}
}

pub async fn init(args: V2Commands) -> Result<()> {
	let path = ensure_binary_present(args.accept).await?;

	let mut child = tokio::process::Command::new(path)
		.args(&args.args)
		.spawn()
		.context("Could not run 2.0 surrealdb binary file")?;

	let res = child.wait().await?;
	let err = res.code().unwrap_or(-1);
	ensure!(res.success(), "Surrealdb v2 process failed with statuscode: {err}");
	Ok(())
}
