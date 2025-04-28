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
	let subset = if let Some(x) = matches.get_one::<String>("filter") {
		testset.filter_map(|name, _| name.contains(x))
	} else {
		testset
	};

	for test in subset.iter() {
		println!("{}", test.path)
	}

	println!("Found {} tests", subset.len());
	Ok(())
}
