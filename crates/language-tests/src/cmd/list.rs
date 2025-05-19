use anyhow::Result;
use clap::ArgMatches;

use crate::tests::TestSet;

pub async fn run(matches: &ArgMatches) -> Result<()> {
	let path: &String = matches.get_one("path").unwrap();
	let (testset, errors) = TestSet::collect_directory(path).await?;
	if !errors.is_empty() {
		println!(" Failed to load some of the tests");
	}
	for err in errors {
		println!("{err:?}");
	}
	let subset = if let Some(filters) = matches.get_many::<String>("filter") {
		let filters: Vec<String> = filters.map(|x| x.to_string()).collect();

		testset.filter_map(|name, _| {
			for filter in &filters {
				if name.contains(filter) {
					return true;
				}
			}

			false
		})
	} else {
		testset
	};

	for test in subset.iter() {
		println!("{}", test.path)
	}

	println!("Found {} tests", subset.len());
	Ok(())
}
