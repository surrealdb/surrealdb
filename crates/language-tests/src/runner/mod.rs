use std::{future::Future, sync::Arc};

use tokio::{sync::Semaphore, task::JoinSet};

pub struct Schedular {
	job_lock: Arc<Semaphore>,
	max_jobs: u32,
	join: JoinSet<()>,
}

pub struct SemaphoreGuard {
	semaphore: Arc<Semaphore>,
	count: u32,
}

impl Drop for SemaphoreGuard {
	fn drop(&mut self) {
		self.semaphore.add_permits(self.count as usize);
	}
}

impl Schedular {
	pub fn new(max_jobs: u32) -> Self {
		assert!(max_jobs > 0);
		Schedular {
			max_jobs,
			job_lock: Arc::new(Semaphore::new(max_jobs as usize)),
			join: JoinSet::new(),
		}
	}

	pub async fn spawn<F>(&mut self, f: F)
	where
		F: Future<Output = ()> + Send + 'static,
	{
		// never call close, so this should not panic.
		self.job_lock.acquire().await.unwrap().forget();
		let guard = SemaphoreGuard {
			semaphore: self.job_lock.clone(),
			count: 1,
		};
		self.join.spawn(async move {
			let _guard = guard;
			f.await;
		});
	}

	pub async fn spawn_sequential<F>(&mut self, f: F)
	where
		F: Future<Output = ()> + Send + 'static,
	{
		let max_jobs = self.max_jobs;
		self.job_lock.acquire_many(max_jobs).await.unwrap().forget();
		let guard = SemaphoreGuard {
			semaphore: self.job_lock.clone(),
			count: max_jobs,
		};
		// never call close, so this should not panic.
		self.join.spawn(async move {
			let _guard = guard;
			f.await;
		});
	}

	pub async fn join_all(mut self) {
		while let Some(x) = self.join.join_next().await {
			x.expect("Run task paniced!");
		}
	}
}
