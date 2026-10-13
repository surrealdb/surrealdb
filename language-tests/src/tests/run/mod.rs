use std::sync::Arc;

use crate::tests::TestLoadError;
use crate::tests::case::{CaseSet, TestCase};

pub trait RunConfig {
	fn name(&self, case: &CaseImports) -> String;
}

#[derive(Debug)]
pub struct CaseImports {
	pub test: Arc<TestCase>,
	pub imports: Vec<Arc<TestCase>>,
	/// Resolved `[test.chain] setup` script, dependencies first. Empty unless
	/// the case declares a chain.
	///
	/// Resolved for every build so a missing path is reported as a load error
	/// whichever runner is in use, but read only by the upgrade runner.
	#[cfg_attr(not(feature = "upgrade"), allow(dead_code))]
	pub chain_setup: Vec<Arc<TestCase>>,
	/// Resolved `[test.chain] mutate` script, dependencies first.
	#[cfg_attr(not(feature = "upgrade"), allow(dead_code))]
	pub chain_mutate: Vec<Arc<TestCase>>,
	/// Resolved `[[test.repair]] script`s, positionally aligned with the
	/// declarations in `test.repair`.
	#[cfg_attr(not(feature = "upgrade"), allow(dead_code))]
	pub repairs: Vec<Vec<Arc<TestCase>>>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct TestRunId(usize);

impl TestRunId {
	#[allow(dead_code)]
	pub fn new(id: usize) -> Self {
		TestRunId(id)
	}
}

/// A single instance of test being run with a given configuration of datastore.
#[derive(Debug)]
pub struct TestRun<T: RunConfig> {
	pub id: TestRunId,
	pub case: Arc<CaseImports>,
	pub config: T,
}

impl<T: RunConfig> TestRun<T> {
	pub fn name(&self) -> String {
		self.config.name(&self.case)
	}
}

type FilterFn<'a> = Box<dyn FnMut(&CaseImports) -> bool + 'a>;
type ExpandFn<'a, T> = Box<dyn FnMut(&CaseImports) -> Vec<T> + 'a>;

pub struct RunSetBuilder<'set, 'error, 'a, T: RunConfig> {
	set: &'set CaseSet,
	errors: &'error mut Vec<TestLoadError>,
	filters: Vec<FilterFn<'a>>,
	expanders: Vec<ExpandFn<'a, T>>,
}

impl<'set, 'error, 'a, T: RunConfig> RunSetBuilder<'set, 'error, 'a, T> {
	pub fn new(set: &'set CaseSet, errors: &'error mut Vec<TestLoadError>) -> Self {
		RunSetBuilder {
			set,
			errors,
			filters: Vec::new(),
			expanders: Vec::new(),
		}
	}

	pub fn with_filter<F>(mut self, filter: F) -> Self
	where
		F: FnMut(&CaseImports) -> bool + 'a,
	{
		self.filters.push(Box::new(filter));
		self
	}

	pub fn with_expander<F>(mut self, expander: F) -> Self
	where
		F: FnMut(&CaseImports) -> Vec<T> + 'a,
	{
		self.expanders.push(Box::new(expander));
		self
	}

	pub fn build(mut self) -> Vec<TestRun<T>> {
		let mut runs = Vec::new();

		for case in self.set.iter() {
			// Resolve imports transitively (imports-of-imports included).
			let Some(imports) = self.set.resolve_imports(
				&case.config.parsed.env.imports,
				case.id,
				&case.origin,
				self.errors,
			) else {
				continue;
			};

			// Chain and repair scripts resolve through the same machinery as
			// `[env] imports`, so they pick up transitive imports and report a
			// missing path as a load error rather than an empty run.
			let mut resolve_one = |path: &str| {
				self.set.resolve_imports(
					std::slice::from_ref(&path.to_owned()),
					case.id,
					&case.origin,
					self.errors,
				)
			};

			let chain = case.config.parsed.test.chain.as_ref();
			let chain_setup = match chain.map(|c| resolve_one(&c.setup)) {
				None => Vec::new(),
				Some(Some(x)) => x,
				Some(None) => continue,
			};
			let chain_mutate = match chain.and_then(|c| c.mutate.as_deref()).map(&mut resolve_one) {
				None => Vec::new(),
				Some(Some(x)) => x,
				Some(None) => continue,
			};

			let mut repairs = Vec::with_capacity(case.config.parsed.test.repair.len());
			let mut repair_failed = false;
			for repair in case.config.parsed.test.repair.iter() {
				match resolve_one(&repair.script) {
					Some(x) => repairs.push(x),
					None => {
						repair_failed = true;
						break;
					}
				}
			}
			if repair_failed {
				continue;
			}

			let case_imports = CaseImports {
				test: case.clone(),
				imports,
				chain_setup,
				chain_mutate,
				repairs,
			};

			if !self.filters.iter_mut().all(|x| x(&case_imports)) {
				continue;
			}

			let case_imports = Arc::new(case_imports);

			for x in self.expanders.iter_mut() {
				for r in x(&case_imports) {
					runs.push(TestRun {
						id: TestRunId(runs.len()),
						case: case_imports.clone(),
						config: r,
					});
				}
			}
		}

		runs
	}
}
