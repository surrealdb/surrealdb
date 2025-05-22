use std::{
	io,
	path::{Path, PathBuf},
	sync::atomic::{AtomicUsize, Ordering},
	time::SystemTime,
};

pub struct TempDir {
	path: Option<PathBuf>,
	id_gen: AtomicUsize,
}

fn xorshift(state: &mut u32) -> u32 {
	let mut x = *state;
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	*state = x;
	x
}

impl TempDir {
	pub async fn new(prefix: &str) -> Result<Self, io::Error> {
		let temp_dir = std::env::temp_dir();

		let time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
		let time = time.as_secs() ^ time.subsec_nanos() as u64;
		let mut state = (time >> 32) as u32 ^ time as u32;

		let rand = xorshift(&mut state);

		let mut dir = temp_dir.join(format!("{prefix}_{rand}"));
		while tokio::fs::metadata(&dir).await.is_ok() {
			let rand = xorshift(&mut state);
			dir = temp_dir.join(format!("{prefix}_{rand}"));
		}

		tokio::fs::create_dir(&dir).await?;

		Ok(TempDir {
			path: Some(dir),
			id_gen: AtomicUsize::new(0),
		})
	}

	pub fn path(&self) -> &Path {
		self.path.as_ref().unwrap().as_path()
	}

	pub fn sub_dir_path(&self) -> PathBuf {
		let id = self.id_gen.fetch_add(1, Ordering::AcqRel);
		self.path().join(format!("sub_dir_{id}"))
	}

	pub async fn cleanup(mut self) -> Result<(), io::Error> {
		tokio::fs::remove_dir_all(&self.path.take().unwrap()).await
	}

	pub fn keep(mut self) {
		self.path = None;
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		if let Some(path) = self.path.take() {
			let _ = std::fs::remove_dir_all(path);
		}
	}
}
