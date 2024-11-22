use std::{future::Future, sync::Arc};

use tokio::{sync::Semaphore, task::JoinSet};

pub struct Schedular {
	job_lock: Arc<Semaphore>,
	max_jobs: u32,
	spawn_lock: Semaphore,
	join: JoinSet<()>,
}

impl Schedular {
	pub fn new(max_jobs: u32) -> Self {
		assert!(max_jobs > 0);
		Schedular {
			max_jobs,
			job_lock: Arc::new(Semaphore::new(max_jobs as usize)),
			spawn_lock: Semaphore::new(max_jobs as usize * 16),
			join: JoinSet::new(),
		}
	}

	pub async fn spawn<F: Future>(&mut self, f: F)
	where
		F: Future<Output = ()> + Send + 'static,
	{
		// never call close, so this should not panic.
		self.spawn_lock.acquire().await.unwrap().forget();
		let lock = self.job_lock.clone();
		self.join.spawn(async move {
			lock.add_permits(1);
			f.await
		});
	}

	pub async fn spawn_sequential<F: Future>(&mut self, f: F)
	where
		F: Future<Output = ()> + Send + 'static,
	{
		let max_jobs = self.max_jobs;
		self.spawn_lock.acquire_many(max_jobs).await.unwrap().forget();
		// never call close, so this should not panic.
		let lock = self.job_lock.clone();
		self.join.spawn(async move {
			lock.add_permits(max_jobs as usize);
			f.await
		});
	}

	pub async fn join(self) {
		self.join.join_all().await;
	}
}
