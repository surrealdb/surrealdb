use anyhow::Result;
use clap::ArgMatches;

use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::{CaseSet, RunSetBuilder};

struct ListConfig;

impl RunConfig for ListConfig {
	fn name(&self, case: &CaseImports) -> String {
		case.test.origin.path.clone()
	}
}

pub async fn run(matches: &ArgMatches) -> Result<()> {
	let mut load_errors = Vec::new();

	let path: &String = matches.get_one("path").unwrap();

	let set = CaseSet::load_surrealql_files(path, &mut load_errors).await?;

	let runs = {
		let set_builder =
			RunSetBuilder::new(&set, &mut load_errors).with_expander(|_| vec![ListConfig]);

		let set_builder = if let Some(name_filter) = matches.get_one::<String>("filter") {
			set_builder.with_filter(move |x| x.test.origin.path.contains(name_filter))
		} else {
			set_builder
		};
		set_builder.build()
	};

	println!("Found {} tests cases", runs.len());
	Ok(())
}
