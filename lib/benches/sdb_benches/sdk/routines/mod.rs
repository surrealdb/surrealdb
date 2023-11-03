use criterion::{measurement::WallTime, Bencher};
use surrealdb::{engine::any::Any, Surreal};

mod create;
pub(super) use create::*;
mod read;
pub(super) use read::*;

/// Routine trait for the benchmark routines.
///
/// The `setup` function is called once before the benchmark starts. It's used to prepare the database for the benchmark.
/// The `run` function is called for each iteration of the benchmark.
/// The `cleanup` function is called once after the benchmark ends. It's used to clean up the database after the benchmark.
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
	// Setup
	routine.setup(db, num_ops.clone());

	// Run the runtime and return the duration for each operation
	b.iter_custom(|iters| {
		let num_ops = num_ops.clone();

		let now = std::time::Instant::now();
		for _ in 0..iters {
			let num_ops = num_ops.clone();

			// Run and time the routine
			routine.run(db, num_ops.clone());
		}

		now.elapsed().div_f32(num_ops as f32)
	});

	// Cleanup the database
	routine.cleanup(db, num_ops);
}
