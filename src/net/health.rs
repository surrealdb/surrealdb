use crate::err::Error;
use axum::routing::get;
use axum::Router;
use axum::{response::IntoResponse, Extension};
use http_body::Body as HttpBody;
use surrealdb::kvs::{LockType::*, TransactionType::*};

use super::AppState;

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/health", get(handler))
}

async fn handler(Extension(state): Extension<AppState>) -> impl IntoResponse {
	// Attempt to open a transaction
	match state.datastore.transaction(Read, Optimistic).await {
		// The transaction failed to start
		Err(_) => Err(Error::InvalidStorage),
		// The transaction was successful
		Ok(mut tx) => {
			// Cancel the transaction
			trace!("Health endpoint cancelling transaction");
			let _ = tx.cancel().await;
			// Return the response
			Ok(())
		}
	}
}
