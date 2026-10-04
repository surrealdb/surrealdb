use std::collections::HashMap;
use std::ops::{Index, Range};
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Error, Result};
use tokio::fs;

use crate::tests::TestLoadError;
use crate::tests::case::config::CaseConfig;
use crate::util::walk_directory;

mod config;

/// A unique id identifying a specific test case.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct CaseId(usize);

impl CaseId {
	/// Construct a [`CaseId`] directly. Only used by the harness's own unit tests,
	/// which build [`TestCase`]s by hand rather than loading them from disk.
	#[cfg(test)]
	pub fn new(id: usize) -> Self {
		CaseId(id)
	}
}

/// A origin of a test, which is some path + possibly an offset within the file at that path.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Origin {
	/// The path of the test relative to the root from which the test was parsed.
	pub path: String,
	/// Last time the test was changed, used for caching datasets.
	pub modified: SystemTime,
	/// A subset of the file at the above path
	/// Used when a testcase can only be a part of a file like when testing the docs.
	pub subset: Option<Range<usize>>,
	pub line_offset: Option<usize>,
}

/// The query language a test case is written in, derived from the file
/// extension: `.surql` is SurrealQL, `.gql` is GQL, `.graphql` is GraphQL,
/// `.pgsql` is PostgreSQL, `.mysql` is MySQL.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Dialect {
	SurrealQl,
	Gql,
	GraphQl,
	PgSql,
	MySql,
}

/// A single test case, which might produce multiple test runs depending on configuration.
#[derive(Debug)]
pub struct TestCase {
	pub id: CaseId,
	pub origin: Arc<Origin>,
	pub config: CaseConfig,
	/// The query language the test source is written in.
	pub dialect: Dialect,
	/// The query source for the test, in the language given by `dialect`.
	/// Includes the config.
	pub source: String,
}

impl TestCase {
	pub fn from_source_origin_id(
		id: CaseId,
		origin: Arc<Origin>,
		source: String,
		dialect: Dialect,
	) -> Result<Self> {
		let config = CaseConfig::parse(&source, dialect).with_context(|| {
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
			dialect,
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
	// Used when upgrade feature is enabled.
	#[allow(unused)]
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

	/// Resolve a list of import paths transitively (imports-of-imports included),
	/// returning the ordered, de-duplicated list of import cases with each
	/// dependency placed before the file that imports it.
	///
	/// `importing` and `origin` identify the file whose imports are being
	/// resolved (used for relative-path resolution and error attribution).
	/// Returns `None` if any import could not be resolved (errors are pushed to
	/// `errors`).
	pub fn resolve_imports(
		&self,
		import_paths: &[String],
		importing: CaseId,
		origin: &Arc<Origin>,
		errors: &mut Vec<TestLoadError>,
	) -> Option<Vec<Arc<TestCase>>> {
		let mut out = Vec::new();
		let mut visited = Vec::new();
		if self.resolve_imports_into(
			import_paths,
			importing,
			origin,
			errors,
			&mut visited,
			&mut out,
		) {
			Some(out)
		} else {
			None
		}
	}

	/// Recursive worker for [`resolve_imports`](Self::resolve_imports).
	///
	/// Appends each resolved import to `out` with dependencies first — an
	/// import's own `[env].imports` are resolved before the import itself —
	/// using `visited` (a list of [`CaseId`] indices) to de-duplicate shared
	/// imports and guard against cycles. Missing or ambiguous imports push a
	/// [`TestLoadError`] onto `errors`. Returns `true` only if every import
	/// resolved.
	fn resolve_imports_into(
		&self,
		import_paths: &[String],
		importing: CaseId,
		origin: &Arc<Origin>,
		errors: &mut Vec<TestLoadError>,
		visited: &mut Vec<usize>,
		out: &mut Vec<Arc<TestCase>>,
	) -> bool {
		let mut ok = true;
		for import in import_paths {
			match self.find_import(import, importing) {
				Some(x) if x.len() == 1 => {
					let imp = x[0].clone();
					// Imports are executed as SurrealQL (`Datastore::execute` on
					// the raw source), so non-SurrealQL files cannot be imported.
					if imp.dialect != Dialect::SurrealQl {
						errors.push(TestLoadError {
							origin: origin.clone(),
							error: Error::msg(format!(
								"Import `{import}` is not a SurrealQL file; imports must be SurrealQL (.surql) files"
							)),
						});
						ok = false;
						continue;
					}
					// Dedup + cycle guard: skip anything already added or in progress.
					if visited.contains(&imp.id.0) {
						continue;
					}
					visited.push(imp.id.0);
					// Resolve this import's own imports first so dependencies run
					// before the file that depends on them.
					let nested = &imp.config.parsed.env.imports;
					if !nested.is_empty()
						&& !self.resolve_imports_into(
							nested,
							imp.id,
							&imp.origin,
							errors,
							visited,
							out,
						) {
						ok = false;
					}
					out.push(imp);
				}
				Some(_) => {
					errors.push(TestLoadError {
						origin: origin.clone(),
						error: Error::msg(format!(
							"Import `{import}` refered to a file which contained multiple tests"
						)),
					});
					ok = false;
				}
				None => {
					errors.push(TestLoadError {
						origin: origin.clone(),
						error: Error::msg(format!("Could not find import `{import}`")),
					});
					ok = false;
				}
			}
		}
		ok
	}

	/// Loads every `.surql` (SurrealQL), `.gql` (GQL) and `.graphql`
	/// (GraphQL) file under `root` (recursively) into a [`CaseSet`].
	///
	/// Each file's config comment is parsed into a [`TestCase`] keyed by its path
	/// relative to `root`. Files whose config fails to parse are recorded in
	/// `errors` (with their [`Origin`]) and skipped rather than aborting the whole
	/// load, so one bad file doesn't hide the rest. Files with other extensions
	/// are ignored.
	pub async fn load_surrealql_files(root: &str, errors: &mut Vec<TestLoadError>) -> Result<Self> {
		let mut cases = Vec::new();
		let mut by_path = HashMap::new();

		let mut root = root.to_string();
		if !root.ends_with("/") {
			root.push('/');
		}

		walk_directory(&root, &mut async |path: &str| {
			let dialect = if path.ends_with(".surql") {
				Dialect::SurrealQl
			} else if path.ends_with(".gql") {
				Dialect::Gql
			} else if path.ends_with(".graphql") {
				Dialect::GraphQl
			} else if path.ends_with(".pgsql") {
				Dialect::PgSql
			} else if path.ends_with(".mysql") {
				Dialect::MySql
			} else {
				return Ok(());
			};

			let metadata = fs::metadata(path).await.context("Could not read file metadata")?;

			let modified = metadata.modified().context("Could not read file modification time")?;

			let source = fs::read_to_string(path)
				.await
				.with_context(|| format!("Could not read test file: {path}"))?;

			assert!(path.starts_with(&root));
			let path = &path[root.len()..];

			let origin = Arc::new(Origin {
				path: path.to_owned(),
				modified,
				subset: None,
				line_offset: None,
			});

			let id = CaseId(cases.len());

			match TestCase::from_source_origin_id(id, origin.clone(), source, dialect) {
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
