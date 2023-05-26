use crate::err::Error;
use crate::iam::verify::{basic, token};
use crate::iam::BASIC;
use crate::iam::TOKEN;
use std::net::SocketAddr;
use std::sync::Arc;
use surrealdb::dbs::{Session, Auth};
use warp::Filter;

pub fn build() -> impl Filter<Extract = (Session,), Error = warp::Rejection> + Clone {
	// Enable on any path
	let conf = warp::any();
	// Add remote ip address
	let conf = conf.and(warp::filters::addr::remote());
	// Add remote ip address
	let conf = conf.map(|addr: Option<SocketAddr>| addr.map(|v| v.to_string()));
	// Add authorization header
	let conf = conf.and(warp::header::optional::<String>("authorization"));
	// Add http origin header
	let conf = conf.and(warp::header::optional::<String>("origin"));
	// Add session id header
	let conf = conf.and(warp::header::optional::<String>("id"));
	// Add namespace header
	let conf = conf.and(warp::header::optional::<String>("ns"));
	// Add database header
	let conf = conf.and(warp::header::optional::<String>("db"));
	// Process all headers
	conf.and_then(process)
}

async fn process(
	ip: Option<String>,
	au: Option<String>,
	or: Option<String>,
	id: Option<String>,
	ns: Option<String>,
	db: Option<String>,
) -> Result<Session, warp::Rejection> {
	// TODO(sgirones): ns and db allow the invalid value Some("").
	// Once we introduce namespaces/databases/scope as resources, we can properly parse the values and reject them if necessary.
	// For now, reject the request if the value is Some("").
	if ns == Some(String::new()) || db == Some(String::new()) {
		Err(Error::InvalidAuth)?
	}

	// Create session
	#[rustfmt::skip]
	let mut session = Session { ip, or, id, ns, db, ..Default::default() };
	// Parse the authentication header
	match au {
		// Basic authentication data was supplied
		Some(auth) if auth.starts_with(BASIC) => basic(&mut session, auth).await,
		// Token authentication data was supplied
		Some(auth) if auth.starts_with(TOKEN) => token(&mut session, auth).await,
		// Wrong authentication data was supplied
		Some(_) => Err(Error::InvalidAuth),
		// No authentication data was supplied
		None => {
			// If auth is disabled, grant access to all
			if !Auth::is_enabled() {
				session.au = Arc::new(Auth::Kv);
			}
			Ok(())
		},
	}?;
	// Pass the authenticated session through
	Ok(session)
}
