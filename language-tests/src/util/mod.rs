use std::mem;
use std::time::SystemTime;

use anyhow::{Context as _, Result, anyhow};
use tokio::fs;

mod temp_dir;
#[allow(unused_imports)]
pub use temp_dir::TempDir;

/// Walk a directory, calling the callback for every file in the directory.
pub async fn walk_directory<F>(root: &str, cb: &mut F) -> Result<()>
where
	F: AsyncFnMut(&str) -> Result<()>,
{
	let mut dir_entries =
		fs::read_dir(root).await.with_context(|| format!("Failed to read directory '{root}'"))?;

	while let Some(entry) = dir_entries.next_entry().await.transpose() {
		let entry = entry.with_context(|| format!("Failed to read entry in directory '{root}'"))?;

		let p: String = entry
			.path()
			.to_str()
			.ok_or_else(|| anyhow!("Directory contained file with a non utf-8 name"))?
			.to_owned();

		let ft = entry
			.file_type()
			.await
			.with_context(|| format!("Failed to get filetype for path '{p}'"))?;

		// explicitly drop the entry to close the file, preventing hiting file open limits.
		mem::drop(entry);

		if ft.is_dir() {
			Box::pin(walk_directory(&p, cb)).await?;
			continue;
		};

		if ft.is_file() {
			cb(&p).await?;
		}
	}
	Ok(())
}

/// xorshift random number generator.
pub fn xorshift(state: &mut u32) -> u32 {
	let mut x = *state;
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	*state = x;
	x
}

/// Returns a u32 generated from the current time.
/// Used for seeding random numbers.
pub fn get_timestamp() -> u32 {
	let time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
	let time = time.as_secs() ^ time.subsec_nanos() as u64;
	(time >> 32) as u32 ^ time as u32
}
