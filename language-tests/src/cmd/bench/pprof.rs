//! In-process sampling profiler for `bench run` (`bench-pprof` feature).
//!
//! SIGPROF-timer based (pprof-rs), so it works on hosts where kernel perf
//! events are locked down and external profilers cannot attach. Activated
//! by setting `BENCH_PPROF` to an output path prefix: sampling covers
//! exactly the measured region — dataset import and warmup excluded — and
//! a `<prefix>.svg` flamegraph is written when the region ends. Profile a
//! single bench per invocation; a second region in one process would
//! re-register the signal handler.

/// Samples the current process for the lifetime of the region, when the
/// `bench-pprof` feature is compiled in and `BENCH_PPROF` names an output
/// prefix. Otherwise a no-op.
pub(crate) struct RegionProfiler {
	#[cfg(feature = "bench-pprof")]
	guard: Option<(pprof::ProfilerGuard<'static>, String)>,
}

impl RegionProfiler {
	#[cfg(feature = "bench-pprof")]
	pub(crate) fn start() -> Self {
		let guard = std::env::var("BENCH_PPROF")
			.ok()
			.filter(|prefix| !prefix.is_empty())
			.and_then(|prefix| {
				pprof::ProfilerGuardBuilder::default()
					.frequency(997)
					.blocklist(&["libc", "libgcc", "pthread", "vdso"])
					.build()
					.ok()
					.map(|guard| (guard, prefix))
			});
		Self {
			guard,
		}
	}

	#[cfg(not(feature = "bench-pprof"))]
	pub(crate) fn start() -> Self {
		Self {}
	}

	#[cfg(feature = "bench-pprof")]
	pub(crate) fn finish(self) {
		if let Some((guard, prefix)) = self.guard
			&& let Ok(report) = guard.report().build()
		{
			match std::fs::File::create(format!("{prefix}.svg")) {
				Ok(file) => {
					if let Err(e) = report.flamegraph(file) {
						eprintln!("BENCH_PPROF: could not write the flamegraph: {e}");
					} else {
						eprintln!("BENCH_PPROF: wrote {prefix}.svg");
					}
				}
				Err(e) => eprintln!("BENCH_PPROF: could not create {prefix}.svg: {e}"),
			}
		}
	}

	#[cfg(not(feature = "bench-pprof"))]
	pub(crate) fn finish(self) {}
}
