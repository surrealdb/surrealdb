use std::future::Future;

#[derive(Default)]
pub struct Executor<'a> {
	exe: executor::Executor<'a>,
}

impl<'a> Executor<'a> {
	pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
		self.exe.run(future).await
	}
}

impl<'a> js::ExecutorSpawner for &Executor<'a> {
	type JoinHandle = executor::Task<()>;
	fn spawn_executor(self, task: js::Executor) -> Self::JoinHandle {
		self.exe.spawn(task)
	}
}
