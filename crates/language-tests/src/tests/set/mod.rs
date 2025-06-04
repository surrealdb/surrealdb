use anyhow::{Context, Result, anyhow};
use std::{
	borrow::Cow,
	collections::{HashMap, hash_map::Values},
	fmt::Write,
	hash::Hash,
	io::{self, IsTerminal as _},
	mem,
	ops::Index,
	path::{self, Path},
	sync::Arc,
};
use tokio::fs;

use crate::{
	cli::ColorMode,
	format::{IndentFormatter, ansi},
};

use super::{ResolvedImport, TestCase};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct TestId(usize);

/// An error that happened during loading of a test case.
#[derive(Debug)]
pub struct TestLoadError {
	path: String,
	error: anyhow::Error,
}

impl TestLoadError {
	pub fn display(&self, color: ColorMode) {
		let use_color = match color {
			ColorMode::Always => true,
			ColorMode::Never => false,
			ColorMode::Auto => io::stdout().is_terminal(),
		};

		type Fmt<'a> = IndentFormatter<&'a mut String>;

		let mut buffer = String::new();
		let mut f = Fmt::new(&mut buffer, 2);
		f.indent(|f| {
			if use_color {
				writeln!(
					f,
					ansi!(
						" ==> ",
						red,
						"Error",
						reset_format,
						" loading ",
						bold,
						"{}",
						reset_format,
						":"
					),
					self.path
				)?
			} else {
				writeln!(f, " ==> Error Loading {}:", self.path)?
			}

			f.indent(|f| writeln!(f, "{:?}", self.error))
		})
		.unwrap();

		println!("{buffer}");
	}
}

#[derive(Clone)]
pub struct TestSet {
	root: String,
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

	pub fn filter_map<F>(&self, f: F) -> TestSet
	where
		F: Fn(&str, &TestCase) -> bool,
	{
		let map = self
			.map
			.iter()
			.filter(|x| f(x.0.as_str(), &self.all[x.1.0]))
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

	pub fn find_all<S>(&self, name: &S) -> Option<TestId>
	where
		S: AsRef<str>,
	{
		let mut name = Cow::Borrowed(name.as_ref());
		if !name.starts_with(path::MAIN_SEPARATOR) {
			name = Cow::Owned(format!("{}{name}", path::MAIN_SEPARATOR));
		}
		self.all_map.get(name.as_ref()).copied()
	}

	pub async fn collect_directory(path: &str) -> Result<(Self, Vec<TestLoadError>)> {
		let mut all = Vec::new();
		let mut map = HashMap::new();
		let mut errors = Vec::new();
		Self::collect_recursive(path, path, &mut map, &mut all, &mut errors).await?;
		Self::resolve_imports(&mut all, &map, &mut errors);
		let map = Arc::new(map);
		Ok((
			Self {
				root: path.to_string(),
				all_map: map.clone(),
				map,
				all: Arc::new(all),
			},
			errors,
		))
	}

	fn resolve_imports(
		all: &mut [TestCase],
		map: &HashMap<String, TestId>,
		errors: &mut Vec<TestLoadError>,
	) {
		// resolve all import paths.
		for t in all.iter_mut() {
			for import_path in t.config.imports() {
				let mut import_name = Cow::Borrowed(import_path);
				if !import_name.starts_with(path::MAIN_SEPARATOR) {
					import_name = Cow::Owned(format!("{}{import_name}", path::MAIN_SEPARATOR));
				}

				if let Some(resolved) = map.get(import_name.as_ref()) {
					t.imports.push(ResolvedImport {
						id: *resolved,
						path: t.path.clone(),
					});
				} else {
					errors.push(TestLoadError {
						path: t.path.clone(),
						error: anyhow::anyhow!(
							"Could not find import `{}` for test `{}`",
							import_path,
							t.path
						),
					});
					t.contains_error = true;
				}
			}
		}

		// ensure that imports don't have imports themselves.
		for test_index in 0..all.len() {
			let mut contains_error = false;
			for import in all[test_index].imports.iter() {
				if !all[import.id.0].config.imports().is_empty() {
					contains_error = true;
					errors.push(TestLoadError {
						path: all[test_index].path.clone(),
						error: anyhow::anyhow!(
							"Importing test `{}` for test `{}` which contains imports itself is not supported.",
							import.path,
							all[test_index].path
						),
					});
				}
			}
			all[test_index].contains_error |= contains_error;
		}
	}

	async fn collect_recursive(
		dir: &str,
		root: &str,
		map: &mut HashMap<String, TestId>,
		all: &mut Vec<TestCase>,
		errors: &mut Vec<TestLoadError>,
	) -> Result<()> {
		let mut dir_entries = fs::read_dir(dir)
			.await
			.with_context(|| format!("Failed to read test directory '{dir}'"))?;

		while let Some(entry) = dir_entries.next_entry().await.transpose() {
			let entry =
				entry.with_context(|| format!("Failed to read entry in directory '{dir}'"))?;

			let p: String = entry
				.path()
				.to_str()
				.ok_or_else(|| anyhow!("Failed to parse entry to utf-8 string"))?
				.to_owned();

			let ft = entry
				.file_type()
				.await
				.with_context(|| format!("Failed to get filetype for path '{p}'"))?;

			// explicitly drop the entry to close the file, preventing hiting file open limits.
			mem::drop(entry);

			if ft.is_dir() {
				Box::pin(Self::collect_recursive(&p, root, map, all, errors)).await?;
				continue;
			};

			if ft.is_file() {
				let Some("surql") = Path::new(&p).extension().map(|x| x.to_str().unwrap_or(""))
				else {
					continue;
				};

				let text = fs::read(&p)
					.await
					.with_context(|| format!("Failed to read test case file `{p}`"))?;

				let case = match TestCase::from_source_path(p.clone(), text) {
					Ok(x) => x,
					Err(e) => {
						errors.push(TestLoadError {
							path: p,
							error: e,
						});
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
