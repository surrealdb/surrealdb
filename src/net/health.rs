use crate::dbs::DB;
use crate::err::Error;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use http_body::Body as HttpBody;
use surrealdb::kvs::{LockType::*, TransactionType::*};

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/health", get(handler))
}

async fn handler() -> impl IntoResponse {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Attempt to open a transaction
	match db.transaction(Read, Optimistic).await {
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
