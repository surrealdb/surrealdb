use crate::dbs::DB;
use crate::err::Error;
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("health").and(warp::path::end()).and(warp::get()).and_then(handler)
}

async fn handler() -> Result<impl warp::Reply, warp::Rejection> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Attempt to open a transaction
	match db.transaction(false, false).await {
		// The transaction failed to start
		Err(_) => Err(warp::reject::custom(Error::InvalidStorage)),
		// The transaction was successful
		Ok(mut tx) => {
			// Cancel the transaction
			let _ = tx.cancel().await;
			// Return the response
			Ok(warp::reply())
		}
	}
}
