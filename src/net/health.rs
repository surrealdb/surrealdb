use super::AppState;
use crate::err::Error;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Extension;
use axum::Router;
use surrealdb::kvs::{LockType::*, TransactionType::*};

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/health", get(handler))
}

async fn handler(Extension(state): Extension<AppState>) -> impl IntoResponse {
	// Get the datastore reference
	let db = &state.datastore;
	// Attempt to open a transaction
	match db.transaction(Read, Optimistic).await {
		// The transaction failed to start
		Err(_) => Err(Error::InvalidStorage),
		// The transaction was successful
		Ok(tx) => {
			// Cancel the transaction
			trace!("Health endpoint cancelling transaction");
			// Attempt to fetch data
			match tx.get(vec![0x00], None).await {
				Err(_) => {
					// Ensure the transaction is cancelled
					let _ = tx.cancel().await;
					// Return an error for this endpoint
					Err(Error::InvalidStorage)
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
