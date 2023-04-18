use futures::{
	future::IntoFuture, ready, stream::FuturesOrdered, TryFuture, TryFutureExt, TryStream,
};
use pin_project_lite::pin_project;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

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
pub fn try_join_all_buffered<I>(iter: I) -> TryJoinAllBuffered<I::Item, I::IntoIter>
where
	I: IntoIterator,
	I::Item: TryFuture,
{
	let mut input = iter.into_iter();
	let mut active = FuturesOrdered::new();

	while active.len() < crate::cnf::MAX_CONCURRENT_TASKS / 2 {
		if let Some(next) = input.next() {
			active.push_back(TryFutureExt::into_future(next));
		} else {
			break;
		}
	}

	TryJoinAllBuffered {
		input,
		active,
		output: Vec::new(),
	}
}

impl<F, I> Future for TryJoinAllBuffered<F, I>
where
	F: TryFuture,
	I: Iterator<Item = F>,
{
	type Output = Result<Vec<F::Ok>, F::Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
