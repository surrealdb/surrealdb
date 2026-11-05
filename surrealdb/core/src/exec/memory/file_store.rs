//! A temporary store of files: the native host's spill target.
//!
//! The store keeps its runs in a session directory of its own beneath the
//! directory it is given, one file a run, and holds an exclusive lock on a
//! file in that directory for as long as it lives. A process that ends
//! without removing its session leaves the lock free, so opening a store
//! sweeps every session beneath the same directory whose lock it can take:
//! those are runs no live process can read again.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;

use super::store::{
	RunId, RunRead, RunRef, RunWrite, SessionName, Sharing, TempStore, Tier, cannot_share,
	not_readable,
};
use crate::exec::BoxFut;

const SESSION_PREFIX: &str = "surreal-spill-";
const LOCK_FILE: &str = ".lock";

/// Runs kept as files in a session directory. See the module documentation.
pub struct FileTempStore {
	session: PathBuf,
	name: SessionName,
	/// Held locked while the store lives, marking the session as in use.
	_lock: File,
	part_size: usize,
	quota: Option<u64>,
	next: AtomicU64,
}

impl std::fmt::Debug for FileTempStore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("FileTempStore").field("session", &self.session).finish()
	}
}

impl FileTempStore {
	/// A store beneath `parent`, writing parts of `part_size` bytes, with
	/// `quota` bytes per query. Sweeps the sessions beneath `parent` that no
	/// live store holds.
	pub fn open(parent: &Path, part_size: usize, quota: Option<u64>) -> anyhow::Result<Self> {
		std::fs::create_dir_all(parent).with_context(|| {
			format!("Failed to create the spill directory {}", parent.display())
		})?;
		sweep(parent);
		let nonce = web_time::SystemTime::now()
			.duration_since(web_time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or_default();
		let dir_name = format!("{SESSION_PREFIX}{}-{nonce}", std::process::id());
		let session = parent.join(&dir_name);
		std::fs::create_dir(&session).context("Failed to create a spill session directory")?;
		let lock = OpenOptions::new()
			.create(true)
			.truncate(false)
			.write(true)
			.open(session.join(LOCK_FILE))
			.context("Failed to create a spill session's lock")?;
		lock.try_lock().map_err(|e| anyhow::anyhow!("Failed to lock a spill session: {e}"))?;
		Ok(Self {
			session,
			name: SessionName::new(dir_name),
			_lock: lock,
			part_size: part_size.max(1),
			quota,
			next: AtomicU64::new(1),
		})
	}

	/// The directory this store's runs are written in.
	pub fn directory(&self) -> &Path {
		&self.session
	}

	/// Whether `run` was written through this store.
	fn owns(&self, run: &RunRef) -> bool {
		run.session == self.name
	}

	fn path(&self, run: RunId) -> PathBuf {
		self.session.join(format!("run-{run}"))
	}
}

impl Drop for FileTempStore {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.session);
	}
}

/// Remove every session beneath `parent` whose lock no live store holds.
fn sweep(parent: &Path) {
	let Ok(entries) = std::fs::read_dir(parent) else {
		return;
	};
	for entry in entries.flatten() {
		let name = entry.file_name();
		if !name.to_string_lossy().starts_with(SESSION_PREFIX) {
			continue;
		}
		let dir = entry.path();
		let free = match File::open(dir.join(LOCK_FILE)) {
			Ok(lock) => lock.try_lock().is_ok(),
			// A session with no lock file was left before its lock was made.
			Err(e) => e.kind() == std::io::ErrorKind::NotFound,
		};
		if free {
			let _ = std::fs::remove_dir_all(&dir);
		}
	}
}

/// Run blocking file work off the async thread where a runtime allows it.
async fn blocking<T: Send + 'static>(
	work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
	if tokio::runtime::Handle::try_current().is_ok() {
		tokio::task::spawn_blocking(work).await.context("A spill task failed")?
	} else {
		work()
	}
}

impl TempStore for FileTempStore {
	fn part_size(&self) -> usize {
		self.part_size
	}

	fn quota(&self) -> Option<u64> {
		self.quota
	}

	fn session(&self) -> &SessionName {
		&self.name
	}

	fn shares(&self) -> bool {
		false
	}

	fn write_run(&self, sharing: Sharing) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		let id = self.next.fetch_add(1, Ordering::Relaxed);
		let path = self.path(id);
		Box::pin(async move {
			if sharing == Sharing::Shared {
				return Err(cannot_share());
			}
			let created = path.clone();
			let file =
				blocking(move || File::create(&created).context("Failed to create a spilled run"))
					.await?;
			Ok(Box::new(FileRunWrite {
				run: RunRef {
					session: self.name.clone(),
					id,
					tier: Tier::Local,
					bytes: 0,
				},
				file: Some(file),
				path,
				finished: false,
			}) as Box<dyn RunWrite>)
		})
	}

	fn read_run(&self, run: &RunRef) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		let path = self.path(run.id);
		let owned = self.owns(run);
		let part_size = self.part_size;
		Box::pin(async move {
			if !owned {
				return Err(not_readable());
			}
			let file =
				blocking(move || File::open(&path).context("Failed to open a spilled run")).await?;
			Ok(Box::new(FileRunRead {
				file: Some(file),
				part_size,
			}) as Box<dyn RunRead>)
		})
	}

	fn delete_run(&self, run: &RunRef) {
		if self.owns(run) {
			let _ = std::fs::remove_file(self.path(run.id));
		}
	}
}

/// A run's file, removed if the writer is dropped before it finishes.
struct FileRunWrite {
	run: RunRef,
	file: Option<File>,
	path: PathBuf,
	finished: bool,
}

impl Drop for FileRunWrite {
	fn drop(&mut self) {
		if !self.finished {
			self.file.take();
			let _ = std::fs::remove_file(&self.path);
		}
	}
}

impl RunWrite for FileRunWrite {
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>> {
		Box::pin(async move {
			let mut file = self.file.take().context("A spilled run written after a failure")?;
			self.run.bytes += part.len() as u64;
			let file = blocking(move || {
				file.write_all(&part).context("Failed to write a spilled run")?;
				Ok(file)
			})
			.await?;
			self.file = Some(file);
			Ok(())
		})
	}

	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunRef>> {
		Box::pin(async move {
			let mut this = *self;
			if let Some(mut file) = this.file.take() {
				blocking(move || file.flush().context("Failed to write a spilled run")).await?;
			}
			this.finished = true;
			Ok(this.run.clone())
		})
	}
}

struct FileRunRead {
	file: Option<File>,
	part_size: usize,
}

impl RunRead for FileRunRead {
	fn next_part(&mut self) -> BoxFut<'_, anyhow::Result<Option<Vec<u8>>>> {
		Box::pin(async move {
			let Some(mut file) = self.file.take() else {
				return Ok(None);
			};
			let part_size = self.part_size;
			let (file, part) = blocking(move || {
				let mut part = Vec::with_capacity(part_size);
				(&mut file)
					.take(part_size as u64)
					.read_to_end(&mut part)
					.context("Failed to read a spilled run")?;
				Ok((file, part))
			})
			.await?;
			if part.is_empty() {
				return Ok(None);
			}
			self.file = Some(file);
			Ok(Some(part))
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A run reads back the bytes written, in parts no larger than the
	/// store's part size, and is gone once deleted.
	#[tokio::test]
	async fn a_run_reads_back_what_was_written() {
		let dir = tempfile::tempdir().expect("a directory");
		let store = FileTempStore::open(dir.path(), 4, None).expect("a store");
		let mut run = store.write_run(Sharing::Private).await.expect("a run");
		run.write_part(b"abcdef".to_vec()).await.expect("a part");
		run.write_part(b"gh".to_vec()).await.expect("a part");
		let id = run.finish().await.expect("finished");
		let mut read = store.read_run(&id).await.expect("a reader");
		let mut got = Vec::new();
		while let Some(part) = read.next_part().await.expect("a part") {
			assert!(part.len() <= 4);
			got.extend(part);
		}
		assert_eq!(got, b"abcdefgh");
		store.delete_run(&id);
		assert!(store.read_run(&id).await.is_err());
	}

	/// A store names its runs by its session directory, writes them
	/// `Local`, refuses to share, and ignores a run of another session.
	#[tokio::test]
	async fn a_file_store_names_its_runs_by_its_session() {
		let dir = tempfile::tempdir().expect("a directory");
		let store = FileTempStore::open(dir.path(), 4, None).expect("a store");
		let run = store.write_run(Sharing::Private).await.expect("a run");
		let name = run.finish().await.expect("finished");
		let directory = store.directory().file_name().expect("a name").to_string_lossy();
		assert_eq!(name.session.as_str(), directory);
		assert_eq!(name.tier, Tier::Local);
		assert!(!store.shares());
		assert!(store.write_run(Sharing::Shared).await.is_err());
		let foreign = RunRef {
			session: SessionName::new("elsewhere"),
			id: name.id,
			tier: Tier::Shared,
			bytes: 0,
		};
		assert!(store.read_run(&foreign).await.is_err());
		store.delete_run(&foreign);
		assert!(store.read_run(&name).await.is_ok());
	}

	/// Opening a store sweeps a session no live store holds, and leaves one
	/// that a live store holds.
	#[test]
	fn opening_a_store_sweeps_abandoned_sessions() {
		let dir = tempfile::tempdir().expect("a directory");
		let abandoned = dir.path().join(format!("{SESSION_PREFIX}1-1"));
		std::fs::create_dir(&abandoned).expect("a session");
		std::fs::write(abandoned.join("run-1"), b"left behind").expect("a run");
		let live = FileTempStore::open(dir.path(), 4, None).expect("a store");
		assert!(!abandoned.exists());
		let other = FileTempStore::open(dir.path(), 4, None).expect("another store");
		assert!(live.directory().exists());
		drop(other);
		drop(live);
		assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 0);
	}
}
