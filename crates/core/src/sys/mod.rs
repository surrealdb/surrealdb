use std::sync::LazyLock;

use futures::lock::Mutex;
use sysinfo::{Pid, System};

/// The current system environment which is used to
/// periodically fetch and compute the system metrics.
pub static ENVIRONMENT: LazyLock<Mutex<Environment>> =
	LazyLock::new(|| Mutex::new(Environment::default()));

/// The current system information which was acquired
/// from the periodic environment process computation.
pub static INFORMATION: LazyLock<Mutex<Information>> =
	LazyLock::new(|| Mutex::new(Information::default()));

pub async fn refresh() {
	// Get the environment
	let mut environment = ENVIRONMENT.lock().await;
	environment.refresh();
	// Get the system information cache
	let mut information = INFORMATION.lock().await;
	// Update the cached information metrics
	information.cpu_usage = environment.cpu_usage();
	(information.memory_allocated, information.threads) = crate::mem::ALLOC.current_usage();
	information.memory_usage = environment.memory_usage();
	information.load_average = environment.load_average();
	information.physical_cores = environment.physical_cores();
	information.available_parallelism = environment.available_parallelism();
}

/// Cached system utilisation metrics information
#[derive(Default)]
pub struct Information {
	pub available_parallelism: usize,
	pub cpu_usage: f32,
	pub load_average: [f64; 3],
	pub memory_allocated: usize,
	pub threads: usize,
	pub memory_usage: u64,
	pub physical_cores: usize,
}

/// An environment for fetching system utilisation
pub struct Environment {
	sys: System,
	pid: Pid,
}

impl Default for Environment {
	fn default() -> Self {
		Self {
			sys: System::new_all(),
			#[cfg(target_family = "wasm")]
			pid: 0.into(),
			#[cfg(not(target_family = "wasm"))]
			pid: Pid::from(std::process::id() as usize),
		}
	}
}

impl Environment {
	/// Returns the system load average value.
	/// This function returns three numbers,
	/// representing the last 1 minute, 5
	/// minute, and 15 minute periods.
	pub fn load_average(&self) -> [f64; 3] {
		let load = System::load_average();
		[load.one, load.five, load.fifteen]
	}

	/// Fetches the estimate of the available
	/// parallelism of the hardware on which the
	/// database is running. This number often
	/// corresponds to the amount of CPUs, but
	/// it may diverge in various cases.
	pub fn physical_cores(&self) -> usize {
		self.sys.physical_core_count().unwrap_or_default()
	}

	/// Fetches the estimate of the available
	/// parallelism of the hardware on which the
	/// database is running. This number often
	/// corresponds to the amount of CPUs, but
	/// it may diverge in various cases.
	pub fn available_parallelism(&self) -> usize {
		std::thread::available_parallelism().map_or_else(|_| num_cpus::get(), |m| m.get())
	}

	/// Returns the total CPU usage of the system
	/// as a percentage. This may be greater than
	/// 100% if running on a mult-core machine.
	pub fn cpu_usage(&self) -> f32 {
		if let Some(process) = self.sys.process(self.pid) {
			process.cpu_usage()
		} else {
			0.0
		}
	}

	/// Returns the total memory usage (in bytes)
	/// of the system. This is the size of the
	/// memory allocated, not including swap.
	pub fn memory_usage(&self) -> u64 {
		if let Some(process) = self.sys.process(self.pid) {
			process.memory()
		} else {
			0
		}
	}

	/// Refreshes the current process information
	/// with memory and cpu usage details. This
	/// ensures that we only fetch data we need.
	pub fn refresh(&mut self) {
		self.sys.refresh_processes_specifics(
			sysinfo::ProcessesToUpdate::Some(&[self.pid]),
			true,
			sysinfo::ProcessRefreshKind::nothing().with_memory().with_cpu(),
		);
	}
}
