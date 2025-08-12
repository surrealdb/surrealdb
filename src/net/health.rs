use axum::routing::get;
use axum::{Extension, Router};

use super::AppState;
use crate::core::dbs::capabilities::RouteTarget;
use crate::core::kvs::LockType::*;
use crate::core::kvs::TransactionType::*;
use crate::net::error::Error as NetError;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/health", get(handler))
}

async fn handler(Extension(state): Extension<AppState>) -> Result<(), NetError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Health) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Health);
		return Err(NetError::ForbiddenRoute(RouteTarget::Health.to_string()));
	}
	// Attempt to open a transaction
	match db.transaction(Read, Optimistic).await {
		// The transaction failed to start
		Err(_) => Err(NetError::InvalidStorage),
		// The transaction was successful
		Ok(tx) => {
			// Cancel the transaction
			trace!("Health endpoint cancelling transaction");
			// Attempt to fetch data
			match tx.get(&vec![0x00], None).await {
				Err(_) => {
					// Ensure the transaction is cancelled
					let _ = tx.cancel().await;
					// Return an error for this endpoint
					Err(NetError::InvalidStorage)
				}
				Ok(_) => {
					// Ensure the transaction is cancelled
					let _ = tx.cancel().await;
					// Return success for this endpoint
					Ok(())
				}
			}
		}
	}
}
