use crate::dbs::DB;
use crate::err::Error;
use crate::iam::verify::basic;
use crate::iam::BASIC;
use crate::net::client_ip;
use surrealdb::dbs::Session;
use surrealdb::iam::verify::token;
use surrealdb::iam::TOKEN;
use warp::Filter;

pub fn build() -> impl Filter<Extract = (Session,), Error = warp::Rejection> + Clone {
	// Enable on any path
	let conf = warp::any();
	// Add remote ip address
	let conf = conf.and(client_ip::build());
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
	let kvs = DB.get().unwrap();
	// Create session
	#[rustfmt::skip]
	let mut session = Session { ip, or, id, ns, db, ..Default::default() };
	// Parse the authentication header
	match au {
		// Basic authentication data was supplied
		Some(auth) if auth.starts_with(BASIC) => basic(&mut session, auth).await,
		// Token authentication data was supplied
		Some(auth) if auth.starts_with(TOKEN) => {
			token(kvs, &mut session, auth).await.map_err(Error::from)
		}
		// Wrong authentication data was supplied
		Some(_) => Err(Error::InvalidAuth),
		// No authentication data was supplied
		None => Ok(()),
	}?;
	// Pass the authenticated session through
	Ok(session)
}
