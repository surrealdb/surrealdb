use failure::Error;

pub fn init(opts: &clap::ArgMatches) -> Result<(), Error> {
	let pth = opts.value_of("path").unwrap();

	if pth == "memory" {
		info!("Starting kvs store in {}", pth);
	} else {
		info!("Starting kvs store at {}", pth);
	}

	Ok(())
}
