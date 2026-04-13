use std::sync::Arc;

use anyhow::Error;

use crate::{cli::Backend, tests::{case::{CaseSet, TestCase}, schema::NewPlannerStrategyConfig, TestLoadError}};

pub struct TestRunConfig{
	pub planner_config: NewPlannerStrategyConfig,
	pub backend: Backend,
}

pub struct CaseImports{
	pub case: Arc<TestCase>,
	pub imports: Vec<Arc<TestCase>>,
}

#[derive(Clone,Copy,Eq,PartialEq,Debug)]
pub struct TestRunId(usize);

/// A single instance of test being run with a given configuration of datastore.
pub struct TestRun{
	pub id: TestRunId,
	pub case: Arc<CaseImports>,
	pub config: TestRunConfig,
}

impl TestRun{
	pub fn name(&self) -> String{
		format!("{} on {} [{}]", self.case.case.origin.path, self.config.backend,self.config.planner_config)
	}
}

type FilterFn<'a> = Box<dyn FnMut(&CaseImports) -> bool + 'a>;
type ExpandFn<'a> = Box<dyn FnMut(&CaseImports) -> Vec<TestRunConfig> + 'a>;

pub struct RunSetBuilder<'set,'error,'a>{
	set: &'set CaseSet,
	errors: &'error mut Vec<TestLoadError>,
	filters: Vec<FilterFn<'a>>,
	expanders: Vec<ExpandFn<'a>>,
}

impl<'set,'error, 'a> RunSetBuilder<'set,'error,'a>{
	pub fn new(set: &'set CaseSet, errors: &'error mut Vec<TestLoadError>) -> Self{
		RunSetBuilder{
			set,
			errors,
			filters: Vec::new(),
			expanders: Vec::new(),
		}
	}

	pub fn with_filter<F>(mut self, filter: F) -> Self
		where F: FnMut(&CaseImports) -> bool + 'a
	{
		self.filters.push(Box::new(filter));
		self
	}

	pub fn with_expander<F>(mut self, expander: F) -> Self
		where F: FnMut(&CaseImports) -> Vec<TestRunConfig> + 'a
	{
		self.expanders.push(Box::new(expander));
		self
	}

	pub fn build(mut self) -> Vec<TestRun>{
		let mut runs = Vec::new();

		for case in self.set.iter(){

			// TODO: Also resolve imports for imports
			let mut imports = Vec::new();
			let mut had_errors = false;
			for import in case.config.config.env.imports.iter() {
				match self.set.find_import(import, case.id) {
					Some(x) => {
						if x.len() > 1 {
							self.errors.push(TestLoadError{
								origin: case.origin.clone(),
								error: Error::msg(format!("Import `{import}` refered to a file which contained multiple tests"))
							});
							had_errors = true;
						}else{
							imports.push(x[0].clone());
						}
					}
					None => {
						self.errors.push(TestLoadError{
							origin: case.origin.clone(),
							error: Error::msg(format!("Could not find import `{import}`"))
						});
						had_errors = true;
					}
				}
			}

			if had_errors{
				continue;
			}

			let case_imports = CaseImports{
				case: case.clone(),
				imports
			};

			if !self.filters.iter_mut().all(|x| x(&case_imports)){
				continue
			}

			let case_imports = Arc::new(case_imports);

			for x in self.expanders.iter_mut() {
				for r in x(&case_imports) {
					runs.push(TestRun{
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

