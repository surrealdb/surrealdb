use criterion::Bencher;
use criterion::measurement::WallTime;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

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
	fn setup(&self, ds: &'static Surreal<Any>, num_ops: usize);
	fn run(&self, ds: &'static Surreal<Any>, num_ops: usize);
	fn cleanup(&self, ds: &'static Surreal<Any>, num_ops: usize);
}

/// Execute the setup, benchmark the `run` function, and execute the cleanup.
pub(super) fn bench_routine<R>(
	b: &mut Bencher<'_, WallTime>,
	db: &'static Surreal<Any>,
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
		for _ in 0..iters {
			// Setup
			routine.setup(db, num_ops);

			// Run and time the routine
			let now = std::time::Instant::now();
			routine.run(db, num_ops);
			total += now.elapsed();

			// Cleanup the database
			routine.cleanup(db, num_ops);
		}

		total.div_f32(num_ops as f32)
	});
}
