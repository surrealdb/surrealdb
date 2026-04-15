use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::util::{get_timestamp, xorshift};

pub struct TempDir {
	path: Option<PathBuf>,
	id_gen: AtomicUsize,
}

impl TempDir {
	#[allow(dead_code)]
	pub async fn new(prefix: &str) -> Result<Self, io::Error> {
		let temp_dir = std::env::temp_dir();

		let mut state = get_timestamp();

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

	#[allow(dead_code)]
	pub fn path(&self) -> &Path {
		self.path.as_ref().unwrap().as_path()
	}

	#[allow(dead_code)]
	pub fn sub_dir_path(&self) -> PathBuf {
		let id = self.id_gen.fetch_add(1, Ordering::AcqRel);
		self.path().join(format!("sub_dir_{id}"))
	}

	#[allow(dead_code)]
	pub async fn cleanup(mut self) -> Result<(), io::Error> {
		tokio::fs::remove_dir_all(&self.path.take().unwrap()).await
	}

	#[allow(dead_code)]
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
