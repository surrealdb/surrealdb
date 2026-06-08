use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_channel::Sender;
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
