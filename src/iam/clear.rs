use crate::err::Error;
use std::sync::Arc;
use surrealdb::Auth;
use surrealdb::Session;

pub async fn clear(session: &mut Session) -> Result<(), Error> {
	session.au = Arc::new(Auth::No);
	Ok(())
}
