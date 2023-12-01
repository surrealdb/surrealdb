mod helpers;

// We use a separated package so that only these package is using PeakAlloc
mod memory_tests {
	use crate::helpers::new_ds;
	extern crate peak_alloc;
	use peak_alloc::PeakAlloc;
	use surrealdb::dbs::Session;
	use surrealdb::err::Error;
	use surrealdb::syn;

	#[global_allocator]
	static GLOBAL: PeakAlloc = PeakAlloc;

	#[tokio::test]
	// This test controls that memory is stable when we really do (quite) nothing
	async fn nmemory_control_test() -> Result<(), Error> {
		let mut mem = MemCollector::default();
		for _ in 0..10000 {
			mem.collect();
		}
		mem.check()
	}

	#[tokio::test]
	async fn memory_test_dbs() -> Result<(), Error> {
		let dbs = new_ds().await?;
		let ses = Session::owner().with_ns("test").with_db("test");

		let mut mem = MemCollector::default();
		for _ in 0..10000 {
			let res = &mut dbs.execute("SELECT * FROM nothing", &ses, None).await?;
			assert_eq!(res.len(), 1);
			let _ = res.remove(0).result?;
			mem.collect();
		}
		mem.check()
	}

	#[tokio::test]
	// This test controls that memory is stable when we really do (quite) nothing
	async fn memory_test_parser() -> Result<(), Error> {
		let mut mem = MemCollector::default();
		for _ in 0..10000 {
			let ast = syn::parse("SELECT * FROM nothing")?;
			assert_eq!(ast.to_string(), "SELECT * FROM nothing;");
			mem.collect();
		}
		mem.check()
	}

	#[derive(Default)]
	struct MemCollector {
		i: usize,
		start_mem: usize,
		current_mem: usize,
	}

	impl MemCollector {
		fn collect(&mut self) {
			self.i += 1;
			if self.start_mem == 0 {
				self.start_mem = GLOBAL.current_usage();
			} else {
				self.current_mem = GLOBAL.current_usage();
				if self.i % 1000 == 0 {
					println!("{}", GLOBAL.current_usage());
				}
			}
		}

		fn check(&self) -> Result<(), Error> {
			assert!(
				(self.current_mem - self.start_mem) < 100_000,
				"Before: {} - After: {} - Idx: {}",
				self.start_mem,
				self.current_mem,
				self.i
			);
			Ok(())
		}
	}
}
