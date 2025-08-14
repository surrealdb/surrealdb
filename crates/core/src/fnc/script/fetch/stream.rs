use std::future::Future;
use std::pin::Pin;

use async_channel::Receiver;
use futures::{Stream, StreamExt};

/// A newtype struct over receiver implementing the [`Stream`] trait.
pub struct ChannelStream<R>(Pin<Box<Receiver<R>>>);

impl<R> Stream for ChannelStream<R> {
	type Item = R;
	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context,
	) -> std::task::Poll<Option<Self::Item>> {
		self.0.poll_next_unpin(cx)
	}
}

/// A struct representing a Javascript `ReadableStream`.
pub struct ReadableStream<R>(Pin<Box<dyn Stream<Item = R> + Send + Sync>>);

impl<R> ReadableStream<R> {
	pub fn new<S: Stream<Item = R> + Send + Sync + 'static>(stream: S) -> Self {
		ReadableStream::new_box(Box::pin(stream))
	}

	pub fn new_box(stream: Pin<Box<dyn Stream<Item = R> + Send + Sync>>) -> Self {
		ReadableStream(stream)
	}
}

impl<R: Clone + 'static + Send + Sync> ReadableStream<R> {
	/// Turn the current stream into two separate streams.
	pub fn tee(&mut self) -> (ReadableStream<R>, impl Future<Output = ()> + use<R>) {
		// replace the stream with a channel driven by as task.
		// TODO: figure out how backpressure works in the stream API.

		// Unbounded, otherwise when one channel gets awaited it might block forever
		// because the other channel fills up.
		let (send_a, recv_a) = async_channel::unbounded::<R>();
		let (send_b, recv_b) = async_channel::unbounded::<R>();
		let new_stream = Box::pin(recv_a);
		let mut old_stream = std::mem::replace(&mut self.0, new_stream);
		let drive = async move {
			while let Some(item) = old_stream.next().await {
				yield_now!();
				if send_a.send(item.clone()).await.is_err() {
					break;
				}
				if send_b.send(item).await.is_err() {
					break;
				}
			}
		};
		(ReadableStream::new(recv_b), drive)
	}
}

impl<R> Stream for ReadableStream<R> {
	type Item = R;

	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context,
	) -> std::task::Poll<Option<Self::Item>> {
		self.0.poll_next_unpin(cx)
	}
}
