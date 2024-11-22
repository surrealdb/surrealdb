use anyhow::Result;
use camino::Utf8Path;
use clap::ArgMatches;

use crate::tests::TestSet;

pub async fn run(matches: &ArgMatches) -> Result<()> {
    let path: &String = matches.get_one("path").unwrap();
    let testset = TestSet::collect_directory(Utf8Path::new(&path)).await?;
    let subset = if let Some(x) = matches.get_one::<String>("filter") {
        testset.filter(x)
    } else {
        testset
    };

    for test in subset.iter() {
        println!("{}", test.path)
    }

    println!("Found {} tests", subset.len());
    Ok(())
}
