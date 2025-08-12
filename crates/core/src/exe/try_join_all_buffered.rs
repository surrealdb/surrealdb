use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::IntoFuture;
use futures::stream::FuturesOrdered;
use futures::{TryFuture, TryFutureExt, TryStream, ready};
use pin_project_lite::pin_project;

pin_project! {
	/// Future for the [`try_join_all_buffered`] function.
	#[must_use = "futures do nothing unless you `.await` or poll them"]
	pub struct TryJoinAllBuffered<F, I>
	where
		F: TryFuture,
		I: Iterator<Item = F>,
	{
		input: I,
		#[pin]
		active: FuturesOrdered<IntoFuture<F>>,
		output: Vec<F::Ok>,
	}
}

/// Creates a future which represents either an in-order collection of the
/// results of the futures given or a (fail-fast) error.
///
/// Only a limited number of futures are driven at a time.
pub fn try_join_all_buffered<I>(iter: I) -> TryJoinAllBuffered<I::Item, I::IntoIter>
where
	I: IntoIterator,
	I::Item: TryFuture,
{
	#[cfg(target_family = "wasm")]
	let limit: usize = 1;

	#[cfg(not(target_family = "wasm"))]
	let limit: usize = *crate::cnf::MAX_CONCURRENT_TASKS;

	let mut input = iter.into_iter();
	let (lo, hi) = input.size_hint();
	let initial_capacity = hi.unwrap_or(lo);
	let mut active = FuturesOrdered::new();

	while active.len() < limit {
		if let Some(next) = input.next() {
			active.push_back(TryFutureExt::into_future(next));
		} else {
			break;
		}
	}

	TryJoinAllBuffered {
		input,
		active,
		output: Vec::with_capacity(initial_capacity),
	}
}

impl<F, I> Future for TryJoinAllBuffered<F, I>
where
	F: TryFuture,
	I: Iterator<Item = F>,
{
	type Output = Result<Vec<F::Ok>, F::Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let mut this = self.project();
		Poll::Ready(Ok(loop {
			match ready!(this.active.as_mut().try_poll_next(cx)?) {
				Some(x) => {
					if let Some(next) = this.input.next() {
						this.active.push_back(TryFutureExt::into_future(next));
					}
					this.output.push(x)
				}
				None => break mem::take(this.output),
			}
		}))
	}
}

#[cfg(test)]
mod tests {
	use std::future::Future;
	use std::task::Poll;
	use std::time::{Duration, Instant};

	use futures::ready;
	use pin_project_lite::pin_project;
	use rand::{Rng, thread_rng};
	use tokio::time::{Sleep, sleep};

	use super::try_join_all_buffered;

	pin_project! {
		struct BenchFuture {
			#[pin]
			sleep: Sleep,
		}
	}

	impl Future for BenchFuture {
		type Output = Result<usize, &'static str>;

		fn poll(
			self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context,
		) -> std::task::Poll<Self::Output> {
			let me = self.project();
			ready!(me.sleep.poll(cx));
			Poll::Ready(if true {
				Ok(42)
			} else {
				Err("no good")
			})
		}
	}

	/// Returns average # of seconds.
	async fn benchmark_try_join_all<F: Future<Output = Result<Vec<usize>, &'static str>>>(
		try_join_all: fn(Vec<BenchFuture>) -> F,
		count: usize,
	) -> f32 {
		let mut rng = thread_rng();
		let mut total = Duration::ZERO;
		let samples = (250 / count.max(1)).max(10);
		for _ in 0..samples {
			let futures = Vec::from_iter((0..count).map(|_| BenchFuture {
				sleep: sleep(Duration::from_millis(rng.gen_range(0..5))),
			}));
			let start = Instant::now();
			try_join_all(futures).await.unwrap();
			total += start.elapsed();
		}
		total.as_secs_f32() / samples as f32
	}

	#[tokio::test]
	#[ignore]
	async fn comparison() {
		for i in (0..10).chain((20..100).step_by(20)).chain((500..10000).step_by(500)) {
			let unbuffered = benchmark_try_join_all(futures::future::try_join_all, i).await;
			let buffered = benchmark_try_join_all(try_join_all_buffered, i).await;
			println!(
				"with {i:<4} futs, buf. exe. takes {buffered:.4}s = {:>5.1}% the time",
				100.0 * buffered / unbuffered
			);

			if i > 7000 {
				assert!(buffered < unbuffered, "buf: {buffered:.5}s unbuf: {unbuffered:.5}s");
			}
		}
	}
}
