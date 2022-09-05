pub async fn spawn_blocking_non_static<T: Send>(work: impl FnOnce() -> T + Send) -> T {
	let (sender, receiver) = futures::channel::oneshot::channel();
	std::thread::scope(|s| {
		s.spawn(|| {
			sender.send(work()).unwrap_or_else(|_| unreachable!("receiver dropped too early"));
		});
	});
	receiver.await.unwrap_or_else(|_| unreachable!("sender dropped too early"))
}
