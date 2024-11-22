use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use std::{
	collections::{hash_map::Values, HashMap},
	hash::Hash,
	mem,
	ops::Index,
	sync::Arc,
};
use tokio::fs;
use tracing::warn;

use super::TestCase;

pub trait Pattern {
	fn matches(&self, name: &Utf8Path) -> bool;
}

impl Pattern for str {
	fn matches(&self, name: &Utf8Path) -> bool {
		name.as_str().contains(self)
	}
}
impl Pattern for String {
	fn matches(&self, name: &Utf8Path) -> bool {
		name.as_str().contains(self)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TestId(usize);

#[derive(Clone)]
pub struct TestSet {
	root: Utf8PathBuf,
	map: Arc<HashMap<String, TestId>>,
	all_map: Arc<HashMap<String, TestId>>,
	all: Arc<Vec<TestCase>>,
}

impl Index<TestId> for TestSet {
	type Output = TestCase;

	fn index(&self, index: TestId) -> &Self::Output {
		&self.all[index.0]
	}
}

impl TestSet {
	pub fn len(&self) -> usize {
		self.map.len()
	}

	pub fn all_len(&self) -> usize {
		self.all.len()
	}

	pub fn root(&self) -> &Utf8Path {
		self.root.as_path()
	}

	pub fn filter<'a, P: Pattern>(&'a self, pattern: &P) -> TestSet {
		let map = self
			.map
			.iter()
			.filter(|x| pattern.matches(Utf8Path::new(&x.0)))
			.map(|(a, b)| (a.clone(), *b))
			.collect();

		let map = Arc::new(map);

		TestSet {
			root: self.root.clone(),
			all_map: self.all_map.clone(),
			map,
			all: self.all.clone(),
		}
	}

	pub fn find<S>(&self, name: &S) -> Option<TestId>
	where
		S: AsRef<str>,
	{
		let name = name.as_ref();
		self.map.get(name).cloned()
	}

	pub fn find_all<S>(&self, name: &S) -> Option<TestId>
	where
		S: AsRef<str>,
	{
		let name = name.as_ref();
		self.all_map.get(name).cloned()
	}

	pub async fn collect_directory(path: &Utf8Path) -> Result<Self> {
		let mut all = Vec::new();
		let mut map = HashMap::new();
		let root = path.to_path_buf();
		Self::collect_recursive(path, &root, &mut map, &mut all).await?;
		let map = Arc::new(map);
		Ok(Self {
			root,
			all_map: map.clone(),
			map,
			all: Arc::new(all),
		})
	}

	async fn collect_recursive(
		dir: &Utf8Path,
		root: &Utf8Path,
		map: &mut HashMap<String, TestId>,
		all: &mut Vec<TestCase>,
	) -> Result<()> {
		let mut dir_entries = fs::read_dir(dir)
			.await
			.with_context(|| format!("Failed to read test directory '{dir}'"))?;

		while let Some(entry) = dir_entries.next_entry().await.transpose() {
			let entry =
				entry.with_context(|| format!("Failed to read entry in directory '{dir}'"))?;

			let p: Utf8PathBuf = entry.path().try_into()?;

			let ft = entry
				.file_type()
				.await
				.with_context(|| format!("Failed to get filetype for path '{p}'"))?;

			// explicitly drop the entry to close the file, preventing hiting file open limits.
			mem::drop(entry);

			if ft.is_dir() {
				Box::pin(Self::collect_recursive(&p, root, map, all)).await?;
				continue;
			};

			if ft.is_file() {
				let Some("surql") = p.extension() else {
					continue;
				};

				let text = fs::read(&p)
					.await
					.with_context(|| format!("Failed to read test case file `{p}`"))?;

				let case = match TestCase::from_source_path(p.clone(), text) {
					Ok(x) => x,
					Err(e) => {
						warn!("{:?}", e.context(format!("Failed to load test at '{p}'")));
						warn!("Skipping test!");
						continue;
					}
				};

				let idx = all.len();
				all.push(case);
				map.insert(
					p.strip_prefix(root).expect("Path should start with dir").to_string(),
					TestId(idx),
				);
			}
		}
		Ok(())
	}

	pub fn iter(&self) -> Iter {
		Iter {
			map_iter: self.map.values(),
			slice: self.all.as_slice(),
		}
	}

	pub fn iter_ids(&self) -> IterIds {
		IterIds {
			map_iter: self.map.values(),
			slice: self.all.as_slice(),
		}
	}
}

pub struct Iter<'a> {
	map_iter: Values<'a, String, TestId>,
	slice: &'a [TestCase],
}

impl<'a> Iterator for Iter<'a> {
	type Item = &'a TestCase;

	fn next(&mut self) -> Option<Self::Item> {
		let v = self.map_iter.next()?;
		Some(&self.slice[v.0])
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.map_iter.size_hint()
	}

	fn count(self) -> usize
	where
		Self: Sized,
	{
		self.map_iter.count()
	}
}

pub struct IterIds<'a> {
	map_iter: Values<'a, String, TestId>,
	slice: &'a [TestCase],
}

impl<'a> Iterator for IterIds<'a> {
	type Item = (TestId, &'a TestCase);

	fn next(&mut self) -> Option<Self::Item> {
		let v = self.map_iter.next()?;
		Some((*v, &self.slice[v.0]))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.map_iter.size_hint()
	}

	fn count(self) -> usize
	where
		Self: Sized,
	{
		self.map_iter.count()
	}
}
