use std::{future::Future, pin::Pin};

use channel::Receiver;
use futures::{FutureExt, Stream, StreamExt};

pub struct ChannelStream<R>(Receiver<R>);

impl<R> Stream for ChannelStream<R> {
    type Item = R;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.recv().poll_unpin(cx).map(|x| x.ok())
    }
}

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
    pub fn tee(&mut self) -> (ReadableStream<R>, impl Future<Output = ()>) {
        // replace the stream with a channel driven by as task.
        let (send_a, recv_a) = channel::bounded::<R>(16);
        let new_stream = Box::pin(ChannelStream(recv_a.clone()));
        let mut old_stream = std::mem::replace(&mut self.0, new_stream);
        let drive = async move {
            while let Some(item) = old_stream.next().await {
                if send_a.send(item).await.is_err() {
                    break;
                }
            }
        };
        (ReadableStream::new(recv_a), drive)
    }
}

impl<R> Stream for ReadableStream<R> {
    type Item = R;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}
