use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_channel::Sender;
use parking_lot::RwLock;
use surrealdb_datastore::close::{ActionScope, CommitAction};
use surrealdb_types::Notification;

/// A live-query notification paired with the datastore node that owns the subscription.
///
/// Writers create these items while evaluating committed changes. The executor buffers them
/// until the surrounding transaction commits, then hands them to the configured
/// [`MessageBroker`]. A rolled-back or failed transaction drops the buffered items without
/// delivery.
#[derive(Clone, Debug)]
pub struct RoutedNotification {
	target_node: uuid::Uuid,
	notification: Notification,
}

impl RoutedNotification {
	/// Creates a notification addressed to the datastore node that owns the subscription.
	pub(crate) fn new(target_node: uuid::Uuid, notification: Notification) -> Self {
		Self {
			target_node,
			notification,
		}
	}

	/// Returns the target datastore node identifier as raw bytes.
	///
	/// This is the stable interop surface for external brokers that map SurrealDB core node ids
	/// onto their own cluster node-id type. Consumers who want a `Uuid` for display can use
	/// `Uuid::from_bytes(item.target_node_bytes())`.
	pub fn target_node_bytes(&self) -> [u8; 16] {
		*self.target_node.as_bytes()
	}

	/// Consumes the routed item and returns the notification payload.
	pub fn into_notification(self) -> Notification {
		self.notification
	}
}

/// Looks up the public HTTP endpoint registered on each node's catalog row.
///
/// Constructed by the [`Datastore`](crate::kvs::Datastore) once the catalog is available, then
/// handed to a [`MessageBroker`] via [`MessageBroker::attach_routing_context`]. Clustered
/// brokers use the resolver to find a peer's address at delivery time instead of consulting
/// any in-memory cluster topology table.
///
/// Cross-node delivery is a server-side concern; WASM datastores never attach a resolver and
/// the trait is intentionally `Send + Sync`-bound to match the broker, which always is.
pub trait NodeEndpointResolver: Send + Sync + Debug {
	/// Returns `Some(endpoint)` when the `target_node` row exists and carries an
	/// `http_endpoint`, `None` otherwise (including on transient read errors —
	/// the relay drops and logs).
	fn resolve(
		&self,
		target_node: [u8; 16],
	) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>>;
}

/// Routing-time context handed to a broker once after the [`Datastore`] is built.
///
/// Brokers are constructed *before* the [`Datastore`] (because the [`Datastore`] stores them),
/// so anything that needs the catalog has to arrive via this post-construction hook.
#[derive(Clone, Debug)]
pub struct BrokerRoutingContext {
	/// This datastore's own identity, raw bytes of its [`Datastore::id`](crate::kvs::Datastore).
	/// Brokers use it to short-circuit delivery to themselves without an external roundtrip.
	pub local_node_id: [u8; 16],
	/// Catalog-backed lookup for peer endpoints.
	pub endpoint_resolver: Arc<dyn NodeEndpointResolver>,
}

/// Pluggable broker for forwarding live-query events to the node that owns each subscription.
///
/// Implementations are best-effort. Delivery failures must not change the result of the write
/// transaction that produced the notification.
pub trait MessageBroker: Send + Sync + Debug {
	/// Fast-path filter: should the document layer bother emitting a notification for a
	/// subscription owned by `target_node` from a node identified by `node_id`?
	///
	/// The local broker returns `true` only when `node_id == target_node` so single-node and
	/// shared-backend community deployments skip work for non-local subscriptions. Clustered
	/// brokers return `true` for any owner they can route to.
	fn should_emit(&self, node_id: [u8; 16], target_node: [u8; 16]) -> Result<bool>;

	/// Deliver a live query event for the given subscription to its owning node.
	fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

	/// Optional one-time post-construction hook. The Datastore builder calls this after the
	/// catalog is available so cross-node brokers can stash a resolver. Default no-op —
	/// in-process brokers don't need it.
	fn attach_routing_context(&self, _ctx: BrokerRoutingContext) {}
}

/// Local-only live-query broker used by the community server.
///
/// It preserves the historical single-node behaviour: subscriptions owned by other datastore
/// nodes are ignored because there is no relay path in community mode.
#[derive(Clone, Debug)]
pub struct LocalMessageBroker(Sender<Notification>);

impl LocalMessageBroker {
	/// Creates a broker that forwards accepted notifications to the local RPC notification loop.
	pub fn new(channel: Sender<Notification>) -> Arc<Self> {
		Arc::new(Self(channel))
	}
}

impl MessageBroker for LocalMessageBroker {
	fn should_emit(&self, node_id: [u8; 16], target_node: [u8; 16]) -> Result<bool> {
		Ok(node_id == target_node)
	}

	fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			let _ = self.0.send(item.into_notification()).await;
		})
	}
}

/// The datastore's live-query broker, fed through a queue per node it delivers
/// to, each drained in order.
///
/// A commit queues its notifications here once it has applied and before its
/// statement answers, so the notifications of one client's commits are queued
/// in the order of those commits. A subscription is owned by one node, so its
/// notifications all take that node's queue, and one task per queue hands them
/// to the broker one at a time, awaiting each delivery before starting the
/// next: a delivery the broker holds up holds up the ones queued for that node
/// after it, instead of being overtaken by them, and holds up nothing queued
/// for any other node. A queue's task is started by the first notification
/// queued for its node, and ends once this handle is dropped and everything
/// queued before then is delivered.
#[derive(Debug)]
pub(crate) struct OrderedBroker {
	broker: Arc<dyn MessageBroker>,
	/// Each node's queue, once a notification for that node has started it. A
	/// task dropped with the runtime it ran on leaves its queue closed, and the
	/// next notification for that node starts another on the runtime queueing
	/// it.
	queues: RwLock<HashMap<[u8; 16], Sender<RoutedNotification>>>,
}

impl OrderedBroker {
	pub(crate) fn new(broker: Arc<dyn MessageBroker>) -> Arc<Self> {
		Arc::new(Self {
			broker,
			queues: RwLock::new(HashMap::new()),
		})
	}

	/// Queue `item` behind everything queued before it for the same node. Never
	/// waits: the queues are unbounded, and hold what the broker has yet to
	/// take.
	pub(crate) fn enqueue(&self, item: RoutedNotification) {
		let node = item.target_node_bytes();
		let Some(item) = Self::try_enqueue(&self.queues.read(), node, item) else {
			return;
		};
		let mut queues = self.queues.write();
		// Another caller may have started the queue since the read.
		let Some(item) = Self::try_enqueue(&queues, node, item) else {
			return;
		};
		let (sender, receiver) = async_channel::unbounded();
		let broker = Arc::clone(&self.broker);
		common::rt::spawn_detached(async move {
			while let Ok(item) = receiver.recv().await {
				broker.send(item).await;
			}
		});
		// The receiver is held by the task just started, so the queue is open.
		let _ = sender.try_send(item);
		queues.insert(node, sender);
	}

	/// Queue `item` on `node`'s open queue, or hand it back when there is none.
	fn try_enqueue(
		queues: &HashMap<[u8; 16], Sender<RoutedNotification>>,
		node: [u8; 16],
		item: RoutedNotification,
	) -> Option<RoutedNotification> {
		match queues.get(&node) {
			Some(queue) => queue.try_send(item).err().map(|closed| closed.into_inner()),
			None => Some(item),
		}
	}
}

impl MessageBroker for OrderedBroker {
	fn should_emit(&self, node_id: [u8; 16], target_node: [u8; 16]) -> Result<bool> {
		self.broker.should_emit(node_id, target_node)
	}

	/// Queues `item` when called, so it is ordered by the call rather than by
	/// when the returned future is first polled.
	fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		self.enqueue(item);
		Box::pin(std::future::ready(()))
	}

	fn attach_routing_context(&self, ctx: BrokerRoutingContext) {
		self.broker.attach_routing_context(ctx);
	}
}

/// Tell a live subscriber its subscription is gone, once the removal has
/// committed.
///
/// Sending before the commit would tear a client's subscription down for a
/// removal that a rollback then undoes, leaving its rows in storage with nothing
/// listening. Queued on the transaction so the notification and the deletion
/// succeed or fail together.
pub(crate) struct SendKill {
	pub(crate) broker: Arc<dyn MessageBroker>,
	pub(crate) notification: RoutedNotification,
}

impl SendKill {
	pub(crate) fn boxed(
		broker: Arc<dyn MessageBroker>,
		notification: RoutedNotification,
	) -> Box<dyn CommitAction> {
		Box::new(Self {
			broker,
			notification,
		})
	}
}

impl CommitAction for SendKill {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin(async move {
			self.broker.send(self.notification).await;
		})
	}

	/// Announces the removal that queued it, so it goes with that removal.
	fn scope(&self) -> ActionScope {
		ActionScope::SavePoint
	}
}

/// The broker an executor installs on its context: it queues each notification
/// on the executor's channel, and defers the routing decision to `delivery`.
#[derive(Clone, Debug)]
pub(crate) struct DefaultBroker {
	sender: Sender<RoutedNotification>,
	delivery: Arc<dyn MessageBroker>,
}

impl DefaultBroker {
	pub(crate) fn new(
		sender: Sender<RoutedNotification>,
		delivery: Arc<dyn MessageBroker>,
	) -> Arc<Self> {
		Arc::new(Self {
			sender,
			delivery,
		})
	}
}
impl MessageBroker for DefaultBroker {
	fn should_emit(&self, node_id: [u8; 16], target_node: [u8; 16]) -> Result<bool> {
		self.delivery.should_emit(node_id, target_node)
	}

	fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			// If there is an error, we can just ignore it,
			// as it means that the channel was closed.
			let _ = self.sender.send(item).await;
		})
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use surrealdb_types::{Action, Value};

	use super::*;

	/// Holds every delivery to `held` until its gate opens, and records each
	/// delivery as it completes.
	#[derive(Debug)]
	struct HoldingBroker {
		held: [u8; 16],
		gate: tokio::sync::Semaphore,
		delivered: Sender<Notification>,
	}

	impl MessageBroker for HoldingBroker {
		fn should_emit(&self, _: [u8; 16], _: [u8; 16]) -> Result<bool> {
			Ok(true)
		}

		fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
			Box::pin(async move {
				if item.target_node_bytes() == self.held {
					let _ = self.gate.acquire().await.map(|permit| permit.forget());
				}
				let _ = self.delivered.send(item.into_notification()).await;
			})
		}
	}

	fn routed(node: [u8; 16], n: i64) -> RoutedNotification {
		let id = uuid::Uuid::from_bytes(node);
		RoutedNotification::new(
			id,
			Notification::new(id.into(), None, Action::Update, Value::None, Value::from_t(n)),
		)
	}

	/// A node's notifications are delivered in the order they were queued, and
	/// a delivery held up for one node holds up nothing queued for another.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn a_held_node_holds_up_only_its_own_queue() {
		let (a, b) = ([1; 16], [2; 16]);
		let (delivered, received) = async_channel::unbounded();
		let inner = Arc::new(HoldingBroker {
			held: a,
			gate: tokio::sync::Semaphore::new(0),
			delivered,
		});
		let ordered = OrderedBroker::new(Arc::clone(&inner) as Arc<dyn MessageBroker>);
		ordered.enqueue(routed(a, 1));
		ordered.enqueue(routed(b, 2));
		ordered.enqueue(routed(a, 3));
		let next = || async {
			tokio::time::timeout(Duration::from_secs(5), received.recv()).await.unwrap().unwrap()
		};
		assert_eq!(next().await.result, Value::from_t(2i64), "the other node is not held up");
		inner.gate.add_permits(2);
		assert_eq!(next().await.result, Value::from_t(1i64));
		assert_eq!(next().await.result, Value::from_t(3i64));
	}
}
