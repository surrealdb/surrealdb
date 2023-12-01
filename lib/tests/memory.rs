mod helpers;

mod memory_tests {
	use crate::helpers::new_ds;
	extern crate peak_alloc;
	use peak_alloc::PeakAlloc;
	use surrealdb::dbs::Session;
	use surrealdb::err::Error;

	#[global_allocator]
	static GLOBAL: PeakAlloc = PeakAlloc;

	#[tokio::test]
	async fn one_dbs_for_all() -> Result<(), Error> {
		let dbs = new_ds().await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let mut start_memory = 0;
		let mut memory_usage = 0;
		for i in 0..10000 {
			let res = &mut dbs.execute("SELECT * FROM nothing", &ses, None).await?;
			assert_eq!(res.len(), 1);
			let _ = res.remove(0).result?;
			if start_memory == 0 {
				start_memory = GLOBAL.current_usage();
			} else {
				memory_usage = GLOBAL.current_usage();
				if i % 1000 == 0 {
					println!("{}", GLOBAL.current_usage());
				}
			}
		}
		assert!(
			(memory_usage - start_memory) < 100_000,
			"Before: {start_memory} - After: {memory_usage}"
		);
		Ok(())
	}

	#[tokio::test]
	async fn one_dbs_per_request() -> Result<(), Error> {
		let mut start_memory = 0;
		let mut memory_usage = 0;
		for i in 0..10000 {
			let dbs = new_ds().await?;
			let ses = Session::owner().with_ns("test").with_db("test");
			let res = &mut dbs.execute("SELECT * FROM nothing", &ses, None).await?;
			assert_eq!(res.len(), 1);
			let _ = res.remove(0).result?;
			if start_memory == 0 {
				start_memory = GLOBAL.current_usage();
			} else {
				memory_usage = GLOBAL.current_usage();
				if i % 1000 == 0 {
					println!("{}", GLOBAL.current_usage());
				}
			}
		}
		assert!(
			(memory_usage - start_memory) < 100_000,
			"Before: {start_memory} - After: {memory_usage}"
		);
		Ok(())
	}
}
