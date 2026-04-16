use std::collections::HashMap;
use std::ops::{Index, Range};
use std::path::{Component, Path};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::fs;

use crate::tests::TestLoadError;
use crate::tests::case::config::CaseConfig;
use crate::util::walk_directory;

mod config;

/// A unique id identifying a specific test case.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct CaseId(usize);

/// A origin of a test, which is some path + possibly an offset within the file at that path.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Origin {
	/// The path of the test relative to the root from which the test was parsed.
	pub path: String,
	/// A subset of the file at the above path
	/// Used when a testcase can only be a part of a file like when testing the docs.
	pub subset: Option<Range<usize>>,
	pub line_offset: Option<usize>,
}

/// A single test case, which might produce multiple test runs depending on configuration.
#[derive(Debug)]
pub struct TestCase {
	pub id: CaseId,
	pub origin: Arc<Origin>,
	pub config: CaseConfig,
	/// The surrealql source for the test.
	/// Includes the config.
	pub source: String,
}

impl TestCase {
	pub fn from_source_origin_id(id: CaseId, origin: Arc<Origin>, source: String) -> Result<Self> {
		let config = CaseConfig::parse(&source).with_context(|| {
			if let Some(line) = origin.line_offset {
				format!("Could not parse config for test file `{}` at line {line}", origin.path)
			} else {
				format!("Could not parse config for test file `{}`", origin.path)
			}
		})?;

		Ok(Self {
			id,
			origin,
			config,
			source,
		})
	}
}

/// A set of test cases which will then have to be filtered to produce the final set of test runs.
pub struct CaseSet {
	cases: Vec<Arc<TestCase>>,
	by_path: HashMap<String, Vec<Arc<TestCase>>>,
}

impl Index<CaseId> for CaseSet {
	type Output = TestCase;

	fn index(&self, index: CaseId) -> &Self::Output {
		&self.cases[index.0]
	}
}

impl CaseSet {
	pub fn len(&self) -> usize {
		self.cases.len()
	}

	pub fn get_by_path(&self, path: &str) -> Option<&[Arc<TestCase>]> {
		self.by_path.get(path).map(|x| x.as_ref())
	}

	pub fn find_import(&self, import_path: &str, importing: CaseId) -> Option<&[Arc<TestCase>]> {
		let search_path = if import_path.starts_with("./") || import_path.starts_with("../") {
			let test_path = &self[importing].origin.path;
			let mut base_path = Path::new(test_path).parent()?.to_path_buf();
			for comp in Path::new(import_path).components() {
				match comp {
					Component::Prefix(_) | Component::RootDir => {
						unreachable!()
					}
					Component::CurDir => {}
					Component::ParentDir => {
						base_path = base_path.parent()?.to_path_buf();
					}
					Component::Normal(os_str) => base_path = base_path.join(os_str),
				}
			}

			let Some(x) = base_path.to_str() else {
				// All paths were derived from strings so they should convert back to strings.
				unreachable!()
			};
			x.to_string()
		} else {
			import_path.to_string()
		};

		self.get_by_path(&search_path)
	}

	pub fn iter(&self) -> impl Iterator<Item = &Arc<TestCase>> {
		self.cases.iter()
	}

	pub async fn load_surrealql_files(root: &str, errors: &mut Vec<TestLoadError>) -> Result<Self> {
		let mut cases = Vec::new();
		let mut by_path = HashMap::new();

		let mut root = root.to_string();
		if !root.ends_with("/") {
			root.push('/');
		}

		walk_directory(&root, &mut async |path: &str| {
			if !path.ends_with(".surql") {
				return Ok(());
			}

			let source = fs::read_to_string(path)
				.await
				.with_context(|| format!("Could not read test file: {path}"))?;

			assert!(path.starts_with(&root));
			let path = &path[root.len()..];

			let origin = Arc::new(Origin {
				path: path.to_owned(),
				subset: None,
				line_offset: None,
			});

			let id = CaseId(cases.len());

			match TestCase::from_source_origin_id(id, origin.clone(), source) {
				Ok(x) => {
					let case = Arc::new(x);
					by_path.entry(path.to_string()).or_insert_with(Vec::new).push(case.clone());
					cases.push(case);
				}
				Err(e) => {
					errors.push(TestLoadError {
						origin,
						error: e,
					});
				}
			}

			Ok(())
		})
		.await?;

		Ok(CaseSet {
			cases,
			by_path,
		})
	}
}
