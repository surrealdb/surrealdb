use std::{
	any::{Any, TypeId},
	collections::HashMap,
	future::Future,
	marker::PhantomData,
	pin::Pin,
	sync::Arc,
};

use parking_lot::RwLock;
use tokio::sync::broadcast;

pub trait EventSet: Clone + 'static {}
pub trait Event<ES: EventSet>: Clone + Send + 'static {}


#[derive(Clone)]
pub struct Publisher<ES: EventSet> {
	channels: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
	buffer_size: usize,
	_phantom: PhantomData<ES>,
}

impl<ES: EventSet> Publisher<ES> {
	pub fn new(buffer_size: usize) -> Self {
		Self {
			channels: Arc::new(RwLock::new(HashMap::new())),
			buffer_size,
			_phantom: PhantomData,
		}
	}

	pub fn publish<E: Event<ES>>(&self, event: E) -> usize {
		let type_id = TypeId::of::<E>();
		let channels = self.channels.read();

		if let Some(sender) = channels.get(&type_id) {
			if let Some(tx) = sender.downcast_ref::<broadcast::Sender<E>>() {
				// broadcast::send returns Ok(count) or Err if no receivers
				return tx.send(event).unwrap_or(0);
			}
		}

		0
	}
}

pub trait Subscribeable<ES: EventSet> {
	fn publisher(&self) -> &Publisher<ES>;
	fn subscribe<E: Event<ES>>(&self) -> broadcast::Receiver<E> {
		let type_id = TypeId::of::<E>();
		let mut channels = self.publisher().channels.write();

		let sender = channels
			.entry(type_id)
			.or_insert_with(|| {
				let (tx, _) = broadcast::channel::<E>(self.publisher().buffer_size);
				Box::new(tx)
			})
			.downcast_ref::<broadcast::Sender<E>>()
			.expect("Type mismatch in event channel registry")
			.clone();

		sender.subscribe()
	}

	fn subscribe_first<E: Event<ES>>(
		&self,
	) -> Pin<Box<dyn Future<Output = Result<E, broadcast::error::RecvError>> + Send + '_>> {
		let mut rx = self.subscribe::<E>();
		Box::pin(async move { rx.recv().await })
	}

	fn pipe<E, TargetES>(&self, to_publisher: Publisher<TargetES>)
	where
		E: Event<ES> + Event<TargetES> + Send + 'static,
		TargetES: EventSet + Send + Sync,
	{
		let mut rx = self.subscribe::<E>();
		tokio::spawn(async move {
			while let Ok(event) = rx.recv().await {
				to_publisher.publish(event);
			}
		});
	}

	fn pipe_filtered<E, TargetES, F>(&self, to_publisher: Publisher<TargetES>, filter: F)
	where
		E: Event<ES> + Event<TargetES> + Send + 'static,
		TargetES: EventSet + Send + Sync,
		F: Fn(&E) -> bool + Send + 'static,
	{
		let mut rx = self.subscribe::<E>();
		tokio::spawn(async move {
			while let Ok(event) = rx.recv().await {
				if filter(&event) {
					to_publisher.publish(event);
				}
			}
		});
	}
}

impl<ES: EventSet> Subscribeable<ES> for Publisher<ES> {
	fn publisher(&self) -> &Publisher<ES> {
		self
	}
}

impl<ES: EventSet, T: Subscribeable<ES>> Subscribeable<ES> for Arc<T> {
	fn publisher(&self) -> &Publisher<ES> {
		T::publisher(self)
	}

	fn subscribe<E: Event<ES>>(&self) -> broadcast::Receiver<E> {
		T::subscribe(self)
	}

	fn subscribe_first<E: Event<ES>>(&self) -> Pin<Box<dyn Future<Output = Result<E, broadcast::error::RecvError>> + Send + '_>> {
		T::subscribe_first(self)
	}
}