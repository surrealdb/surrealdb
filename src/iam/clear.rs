use crate::err::Error;
use std::sync::Arc;
use surrealdb::dbs::Auth;
use surrealdb::dbs::Session;

pub async fn clear(session: &mut Session) -> Result<(), Error> {
	session.au = Arc::new(Auth::No);
	session.tk = None;
	session.sc = None;
	session.sd = None;
	Ok(())
}
