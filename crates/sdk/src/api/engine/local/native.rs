use crate::{
	api::{
		conn, method::BoxFuture, opt::{Endpoint, EndpointKind}, ExtraFeatures, Result, Surreal
	},
	engine::{local::grpc::{ConnectionsState, SurrealDBGrpcService}, tasks},
	opt::{auth::Root, WaitFor},
};
use async_channel::{Receiver, Sender};
use futures::{StreamExt, stream::poll_fn};
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SurrealDbServiceServer;
use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::{Arc, atomic::AtomicI64},
	task::Poll,
};
use surrealdb_core::{dbs::Session, iam::Level, kvs::Datastore, options::EngineOptions};
use tokio::{io::DuplexStream, sync::{watch, RwLock}};
use tokio_util::sync::CancellationToken;

pub(crate) async fn serve(
	// The transport channel for the gRPC server. This will be a DuplexStream when running the server locally and a TcpStream when running as a remote instance.
	channel: DuplexStream,
	// The address of the database to connect to. This will be a URL when running as a remote instance and a path when running locally.
	address: Endpoint,
) -> Result<()> {

	let id = uuid::Uuid::new_v4();
	let session = Session::default().with_rt(true);
	

	let configured_root = match address.config.auth {
		Level::Root => Some(Root {
			username: &address.config.username,
			password: &address.config.password,
		}),
		_ => None,
	};

	let path = address.url.as_str();

	let kvs = match Datastore::new(path).await {
		Ok(kvs) => {
			if let Err(error) = kvs.check_version().await {
				return Err(error);
			};
			if let Err(error) = kvs.bootstrap().await {
				return Err(error);
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = configured_root {
				if let Err(error) = kvs.initialise_credentials(root.username, root.password).await {
					return Err(error);
				}
			}
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			return Err(error);
		}
	};

	let state = Arc::new(ConnectionsState::default());
	let canceller = CancellationToken::new();
	let shutdown = CancellationToken::new();


	let server_task = tokio::spawn(async move {
		tonic::transport::Server::builder()
			.add_service(SurrealDbServiceServer::new(SurrealDBGrpcService::new(
				id,
				session,
				Arc::new(kvs),
				state,
				canceller,
				shutdown,
			)))
			.serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(channel)))
			.await
	});
	todo!("STU: Implement local native router");
	

	// let kvs = match address.config.capabilities.allows_live_query_notifications() {
	// 	true => kvs.with_notifications(),
	// 	false => kvs,
	// };

	// let kvs = kvs
	// 	.with_strict_mode(address.config.strict)
	// 	.with_query_timeout(address.config.query_timeout)
	// 	.with_transaction_timeout(address.config.transaction_timeout)
	// 	.with_capabilities(address.config.capabilities);

	// #[cfg(storage)]
	// let kvs = kvs.with_temporary_directory(address.config.temporary_directory);

	// let kvs = Arc::new(kvs);
	// let vars = Arc::new(RwLock::new(BTreeMap::default()));
	// let live_queries = Arc::new(RwLock::new(HashMap::new()));
	// let session = Arc::new(RwLock::new(Session::default().with_rt(true)));

	// let canceller = CancellationToken::new();

	// let mut opt = EngineOptions::default();
	// if let Some(interval) = address.config.node_membership_refresh_interval {
	// 	opt.node_membership_refresh_interval = interval;
	// }
	// if let Some(interval) = address.config.node_membership_check_interval {
	// 	opt.node_membership_check_interval = interval;
	// }
	// if let Some(interval) = address.config.node_membership_cleanup_interval {
	// 	opt.node_membership_cleanup_interval = interval;
	// }
	// if let Some(interval) = address.config.changefeed_gc_interval {
	// 	opt.changefeed_gc_interval = interval;
	// }
	// let tasks = tasks::init(kvs.clone(), canceller.clone(), &opt);

	// let mut notifications = kvs.notifications().map(Box::pin);
	// let mut notification_stream = poll_fn(move |cx| match &mut notifications {
	// 	Some(rx) => rx.poll_next_unpin(cx),
	// 	// return poll pending so that this future is never woken up again and therefore not
	// 	// constantly polled.
	// 	None => Poll::Pending,
	// });

	// loop {
	// 	let kvs = kvs.clone();
	// 	let session = session.clone();
	// 	let vars = vars.clone();
	// 	let live_queries = live_queries.clone();
	// 	tokio::select! {
	// 		route = route_rx.recv() => {
	// 			let Ok(route) = route else {
	// 				break
	// 			};
	// 			tokio::spawn(async move {
	// 				let result = super::router(route.request, &kvs, &session, &vars, &live_queries)
	// 					.await;

	// 				route.response.send(result).await.ok();
	// 			});
	// 		}
	// 		notification = notification_stream.next() => {
	// 			let Some(notification) = notification else {
	// 				// TODO: Maybe we should do something more then ignore a closed notifications
	// 				// channel?
	// 				continue
	// 			};

	// 			tokio::spawn(async move {
	// 				let id = notification.id.0;
	// 				if let Some(sender) = live_queries.read().await.get(&id) {

	// 					if sender.send(notification).await.is_err() {
	// 						live_queries.write().await.remove(&id);
	// 						if let Err(error) =
	// 							super::kill_live_query(&kvs, id, &*session.read().await, vars.read().await.clone()).await
	// 						{
	// 							warn!("Failed to kill live query '{id}'; {error}");
	// 						}
	// 					}
	// 				}
	// 			});
	// 		}
	// 	}
	// }
	// // Shutdown and stop closed tasks
	// canceller.cancel();
	// // Wait for background tasks to finish
	// tasks.resolve().await.ok();
	// // Delete this node from the cluster
	// kvs.shutdown().await.ok();
}
