//TODO: Remove once cache and other backends are properly implemented.
#![allow(dead_code)]

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use crate::cmd::bench::DEFAULT_NOISE_THRESHOLD;
use crate::cmd::bench::DEFAULT_SIGNIFICANCE_THRESHOLD;
use crate::cmd::bench::stats::ComparisonData;
use crate::cmd::bench::stats::MeasurementData;
use crate::cmd::bench::store::BenchMarkRun;
use crate::cmd::bench::store::StoreConfig;
use crate::cmd::bench::store::get_store;
use crate::cmd::util;
use crate::cmd::util::ImportFailure;
use crate::tests::RunSetBuilder;
use crate::tests::TestRun;
use crate::tests::run::CaseImports;
use crate::tests::run::RunConfig;
use crate::tests::schema::BoolOr;
use crate::tests::schema::NewPlannerStrategyConfig;
use crate::tests::schema::TestConfig;
use crate::{
	cli::{Backend, ColorMode},
	tests::CaseSet,
};
use anyhow::Context;
use anyhow::Result;
use anyhow::{anyhow, bail};
use clap::ArgMatches;
use semver::Version;
use sha2::{Digest, Sha256};
use surrealdb_core::channel;
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::dbs::capabilities::Targets;
use surrealdb_core::env::VERSION;
use surrealdb_core::kvs::Builder;
use surrealdb_core::kvs::Datastore;

struct BenchRunConfig {
	cache_id: String,
}

impl RunConfig for BenchRunConfig {
	fn name(&self, case: &CaseImports) -> String {
		case.test.origin.path.clone()
	}
}

fn calc_cache_id(case: &CaseImports) -> String {
	static BYTES: &[u8] = b"0123456789abcdef";
	let mut hasher = Sha256::new();
	for i in case.imports.iter() {
		hasher.update(i.origin.path.as_bytes());
		let Ok(epoch) = i.origin.modified.duration_since(SystemTime::UNIX_EPOCH) else {
			continue;
		};
		hasher.update(epoch.as_secs().to_le_bytes());
		hasher.update(epoch.subsec_nanos().to_le_bytes());
	}
	let bytes = hasher.finalize();
	let mut res = String::new();
	for b in bytes.iter() {
		res.push(BYTES[(b & 0b1111) as usize] as char);
		res.push(BYTES[(b >> 4) as usize] as char);
	}
	res
}

struct BenchConfig {
	ds_cache: String,
	backend: Backend,
	new_planner: NewPlannerStrategyConfig,
}

struct CmdConfig<'a> {
	path: &'a String,
	backend: Backend,
	ds_cache: &'a String,
	save: bool,
	store: StoreConfig<'a>,
}

impl<'a> CmdConfig<'a> {
	fn from_matches(parent: &'a ArgMatches, current: &'a ArgMatches) -> Self {
		let path: &String = current.get_one("path").unwrap();

		let backend = *current.get_one::<Backend>("backend").unwrap();
		let ds_cache = current.get_one::<String>("ds-cache").unwrap();
		let save = current.get_flag("save");

		Self {
			path,
			backend,
			ds_cache,
			save,
			store: StoreConfig::from_matches(parent),
		}
	}
}

/// Main subcommand function, runs the actual subcommand.
pub async fn run(color: ColorMode, parent: &ArgMatches, current: &ArgMatches) -> Result<()> {
	if cfg!(debug_assertions) {
		println!(
			"Warning, debug assertions are enabled, it is likely the benchmarking suite is build without optimization"
		)
	}

	let mut load_errors = Vec::new();

	let cfg = CmdConfig::from_matches(parent, current);
	let set = CaseSet::load_surrealql_files(cfg.path, &mut load_errors).await?;

	let config = BenchConfig {
		ds_cache: cfg.ds_cache.clone(),
		backend: cfg.backend,
		new_planner: NewPlannerStrategyConfig::BestEffortRo,
	};

	let mut store = get_store(&cfg.store).await?;

	// Check if the backend is supported by the enabled features.
	match cfg.backend {
		Backend::Memory => {}
		#[cfg(feature = "backend-rocksdb")]
		Backend::RocksDb => {}
		#[cfg(not(feature = "backend-rocksdb"))]
		Backend::RocksDb => bail!("RocksDb backend feature is not enabled"),
		#[cfg(feature = "backend-surrealkv")]
		Backend::SurrealKv => {}
		#[cfg(not(feature = "backend-surrealkv"))]
		Backend::SurrealKv => bail!("SurrealKV backend feature is not enabled"),
		Backend::TikV => bail!("TiKV backend is not supported for benchmarking"),
	}

	let core_version = Version::parse(VERSION).unwrap();
	let set_builder = RunSetBuilder::new(&set, &mut load_errors)
		// Only run test for which run is enabled.
		.with_filter(|x| x.test.config.parsed.bench.run)
		// Only run test for this backend.
		.with_filter(|x| {
			let config_backend = &x.test.config.parsed.env.backend;
			config_backend.is_empty() || config_backend.contains(&cfg.backend)
		})
		// Only run for the right version.
		.with_filter(|x| {
			if let Some(x) = &x.test.config.parsed.test.version
				&& !x.matches(&core_version)
			{
				return false;
			}

			if let Some(x) = &x.test.config.parsed.test.importing_version
				&& !x.matches(&core_version)
			{
				return false;
			}

			for i in x.imports.iter() {
				if let Some(x) = &i.config.parsed.test.version
					&& !x.matches(&core_version)
				{
					return false;
				}
			}

			true
		})
		// Run for all config the test has configured.
		.with_expander(|x| {
			vec![BenchRunConfig {
				cache_id: calc_cache_id(x),
			}]
		});

	let set = set_builder.build();

	if set.is_empty() {
		println!("No benchmarks found, exiting");
		return Ok(());
	}

	let mut measurements = Vec::new();
	for i in set.into_iter() {
		let baseline = store
			.fetch_latest(&i.case.test.origin.path, cfg.backend)
			.await
			.context("Could not fetch latest measurement data")?;

		let measurement = thread::scope(|scope| {
			scope
				.spawn(|| {
					tokio::runtime::Builder::new_multi_thread()
						.enable_all()
						.build()
						.unwrap()
						.block_on(run_bench(&i, &config, baseline))
				})
				.join()
		})
		.map_err(|e| {
			if let Some(x) = e.downcast_ref::<String>() {
				anyhow!("Measurement thread paniced: {x}")
			} else {
				anyhow!("Measurement thread paniced")
			}
		})??;

		match measurement {
			BenchRunResult::Import(imp_fail) => {
				println!(
					"Error, import `{}` returned an error: {}",
					imp_fail.path, imp_fail.message
				);
			}
			BenchRunResult::Ok(measurement, compare) => {
				measurements.push((i, measurement, compare));
			}
		}
	}

	for (i, m, compare) in measurements {
		println!(" - {}", i.name());

		if let Some(compare) = compare {
			let signficant = compare.p_value < DEFAULT_SIGNIFICANCE_THRESHOLD;
			if !signficant {
				println!("       No change in performance detected")
			} else {
				let noise = DEFAULT_NOISE_THRESHOLD;
				if compare.dist_mean.lower_bound < -noise && compare.dist_mean.upper_bound < -noise
				{
					println!("       Performance has improved")
				} else if compare.dist_mean.lower_bound > noise
					&& compare.dist_mean.upper_bound > noise
				{
					println!("       Performance has regressed")
				} else {
					println!("       Performance difference within noise threshold")
				}
			}

			fn sign(negative: bool) -> &'static str {
				if negative {
					"-"
				} else {
					""
				}
			}

			let lb = Duration::from_secs_f64(compare.dist_mean.lower_bound.abs());
			let lb_sign = sign(compare.dist_mean.lower_bound.is_sign_negative());
			let ub = Duration::from_secs_f64(compare.dist_mean.upper_bound.abs());
			let ub_sign = sign(compare.dist_mean.upper_bound.is_sign_negative());
			let p = Duration::from_secs_f64(compare.dist_mean.point.abs());
			let p_sign = sign(compare.dist_mean.point.is_sign_negative());

			println!(
				" {:>24} : [{}{:?} {}{:?} {}{:?}] (p = {:.2} {} {:.2})",
				"change",
				lb_sign,
				lb,
				p_sign,
				p,
				ub_sign,
				ub,
				compare.p_value,
				if signficant {
					"<"
				} else {
					">"
				},
				DEFAULT_SIGNIFICANCE_THRESHOLD
			);
		};

		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"time",
			Duration::from_secs_f64(m.mean.lower_bound),
			Duration::from_secs_f64(m.mean.point),
			Duration::from_secs_f64(m.mean.upper_bound),
		);
		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"median",
			Duration::from_secs_f64(m.median.lower_bound),
			Duration::from_secs_f64(m.median.point),
			Duration::from_secs_f64(m.median.upper_bound),
		);
		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"std dev",
			Duration::from_secs_f64(m.std_dev.lower_bound),
			Duration::from_secs_f64(m.std_dev.point),
			Duration::from_secs_f64(m.std_dev.upper_bound),
		);
		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"mad",
			Duration::from_secs_f64(m.abs_dev.lower_bound),
			Duration::from_secs_f64(m.abs_dev.point),
			Duration::from_secs_f64(m.abs_dev.upper_bound),
		);
		let outliers = m.labels.iter().filter(|x| x.is_outlier()).count();
		if outliers != 0 {
			println!(
				"   Found {} outliers, among {} measurements ({:.2}%)",
				outliers,
				m.labels.len(),
				((outliers as f64 / m.labels.len() as f64) * 100.0).round()
			);

			println!(
				"    Low severe  {}",
				m.labels.iter().filter(|x| x.is_low() && x.is_severe()).count()
			);
			println!(
				"    Low mild    {}",
				m.labels.iter().filter(|x| x.is_low() && !x.is_severe()).count()
			);
			println!(
				"    High mild   {}",
				m.labels.iter().filter(|x| x.is_high() && !x.is_severe()).count()
			);
			println!(
				"    High severe {}",
				m.labels.iter().filter(|x| x.is_high() && x.is_severe()).count()
			);
		}

		if cfg.save {
			store
				.add(BenchMarkRun {
					measurement: m,
					path: i.case.test.origin.path.clone(),
					backend: cfg.backend,
				})
				.await
				.context("Could not store latest measurement data")?;
		}
	}

	for e in load_errors.iter() {
		e.display(color);
	}

	store.close().await.context("Failed to close benchmark store")?;

	if !load_errors.is_empty() {
		bail!("Could not load all tests")
	}

	Ok(())
}

pub fn builder_from_config(config: &TestConfig) -> Builder {
	let capabilities = match &config.env.capabilities {
		BoolOr::Bool(true) => Capabilities::all().with_experimental(Targets::All),
		BoolOr::Bool(false) => Capabilities::none(),
		BoolOr::Value(x) => util::core_capabilities_from_test_config(x),
	};

	let builder = Datastore::builder();
	let builder = if capabilities.allows_live_query_notifications() {
		let (send, _) = channel::bounded(15_000);
		builder.with_notify(send)
	} else {
		builder
	};
	builder.with_capabilities(capabilities)
}

#[allow(clippy::large_enum_variant)]
enum BenchRunResult {
	Import(ImportFailure),
	Ok(MeasurementData, Option<ComparisonData>),
}

async fn run_bench(
	run: &TestRun<BenchRunConfig>,
	config: &BenchConfig,
	baseline: Option<MeasurementData>,
) -> Result<BenchRunResult> {
	println!("Running bench {}", run.name());
	println!("Warming up");

	let bench_config = &run.case.test.config.parsed.bench;

	let warmup_time = bench_config.warmup.0;
	let token = tokio_util::sync::CancellationToken::new();

	let before_warmup = Instant::now();
	let mut count = 0usize;
	loop {
		let dbs = Arc::new(
			builder_from_config(&run.case.test.config.parsed).build_with_path("mem").await?,
		);

		let session =
			util::session_from_test_config(&run.case.test.config.parsed, config.new_planner.into());

		if let Some(e) = util::run_imports(run, session.clone(), &dbs).await? {
			return Ok(BenchRunResult::Import(e));
		}

		Datastore::index_compaction(dbs.clone(), Duration::from_secs(1), token.clone()).await?;

		let _ = dbs.execute(&run.case.test.source, &session, None).await?;

		count += 1;

		if before_warmup.elapsed() > warmup_time {
			break;
		}
	}
	let measured_warmup_time = before_warmup.elapsed();

	let expected_iteration_time = measured_warmup_time.as_secs_f64() / count as f64;

	let iterations_per_samples = ((bench_config.measurement_time.0.as_secs_f64()
		/ expected_iteration_time
		/ bench_config.sample_size as f64)
		.ceil() as u64)
		.max(1);

	if iterations_per_samples == 1 {
		println!(
			"Could not complete {} samples in set measurement_time of {:?}",
			bench_config.sample_size, bench_config.measurement_time.0
		);
	}

	let estimate =
		expected_iteration_time * iterations_per_samples as f64 * bench_config.sample_size as f64;

	println!(
		"Completed {count} iterations in {warmup_time:?}, estimated execution time is {:?}",
		Duration::from_secs_f64(estimate)
	);

	let mut iterations = Vec::new();
	let mut samples = Vec::new();
	for _ in 0..bench_config.sample_size {
		let mut sample_duration = 0.0;
		for _ in 0..iterations_per_samples {
			let dbs = Arc::new(
				builder_from_config(&run.case.test.config.parsed).build_with_path("mem").await?,
			);

			let session = util::session_from_test_config(
				&run.case.test.config.parsed,
				config.new_planner.into(),
			);

			if let Some(e) = util::run_imports(run, session.clone(), &dbs).await? {
				return Ok(BenchRunResult::Import(e));
			}

			Datastore::index_compaction(dbs.clone(), Duration::from_secs(1), token.clone()).await?;

			let start = Instant::now();
			let _ = dbs.execute(&run.case.test.source, &session, None).await?;
			sample_duration += start.elapsed().as_secs_f64();
		}
		iterations.push(iterations_per_samples as f64);
		samples.push(sample_duration);
	}

	let measurement = MeasurementData::from_iteration_times(iterations, samples);
	let comp = baseline.map(|baseline| ComparisonData::compare(&baseline, &measurement));

	Ok(BenchRunResult::Ok(measurement, comp))
}
