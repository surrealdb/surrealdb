use anyhow::Result;

use crate::dbs::Session;
use crate::err::Error;

pub fn check_ns_db(sess: &Session) -> Result<(String, String)> {
	// Ensure that a namespace was specified
	let ns = match sess.ns.clone() {
		Some(ns) => ns,
		None => return Err(anyhow::Error::new(Error::NsEmpty)),
	};
	// Ensure that a database was specified
	let db = match sess.db.clone() {
		Some(db) => db,
		None => return Err(anyhow::Error::new(Error::DbEmpty)),
	};
	// All ok
	Ok((ns, db))
}
