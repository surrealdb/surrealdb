use std::sync::Arc;

use criterion::Bencher;
use criterion::measurement::WallTime;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;

mod create;
pub(super) use create::*;
mod read;
pub(super) use read::*;

/// Routine trait for the benchmark routines.
///
/// The `setup` function is called once before the benchmark starts. It's used
/// to prepare the database for the benchmark. The `run` function is called for
/// each iteration of the benchmark. The `cleanup` function is called once after
/// the benchmark ends. It's used to clean up the database after the benchmark.
pub(super) trait Routine {
	fn setup(&self, ds: Arc<Datastore>, session: Session, num_ops: usize);
	fn run(&self, ds: Arc<Datastore>, session: Session, num_ops: usize);
	fn cleanup(&self, ds: Arc<Datastore>, session: Session, num_ops: usize);
}

/// Execute the setup, benchmark the `run` function, and execute the cleanup.
pub(super) fn bench_routine<R>(
	b: &mut Bencher<'_, WallTime>,
	ds: Arc<Datastore>,
	routine: R,
	num_ops: usize,
) where
	R: Routine,
{
	// Run the runtime and return the duration, accounting for the number of
	// operations on each run
	b.iter_custom(|iters| {
		// Total time spent running the actual benchmark run for all iterations
		let mut total = std::time::Duration::from_secs(0);
		let session = Session::owner().with_ns("test").with_db("test");
		for _ in 0..iters {
			// Setup
			routine.setup(ds.clone(), session.clone(), num_ops);

			// Run and time the routine
			let now = std::time::Instant::now();
			routine.run(ds.clone(), session.clone(), num_ops);
			total += now.elapsed();

			// Cleanup the database
			routine.cleanup(ds.clone(), session.clone(), num_ops);
		}

		total.div_f32(num_ops as f32)
	});
}
