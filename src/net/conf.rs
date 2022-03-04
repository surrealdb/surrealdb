use std::net::SocketAddr;
use surrealdb::Session;
use warp::Filter;

pub fn build() -> impl Filter<Extract = (Session,), Error = warp::Rejection> + Copy {
	// Enable on any path
	let conf = warp::any();
	// Add remote ip address
	let conf = conf.and(warp::filters::addr::remote());
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
	conf.map(process)
}

fn process(
	ip: Option<SocketAddr>,
	au: Option<String>,
	or: Option<String>,
	id: Option<String>,
	ns: Option<String>,
	db: Option<String>,
) -> Session {
	// Specify default conf
	let mut conf = Session::default();
	// Specify client ip
	conf.ip = ip.map(|v| v.to_string());
	// Specify session origin
	conf.or = or;
	// Specify session id
	conf.id = id;
	// Specify namespace
	conf.ns = ns;
	// Specify database
	conf.db = db;
	// Parse authentication
	match au {
		Some(auth) if auth.starts_with("Basic") => basic(auth, conf),
		Some(auth) if auth.starts_with("Bearer") => token(auth, conf),
		_ => conf,
	}
}

fn basic(_auth: String, conf: Session) -> Session {
	conf
}

fn token(_auth: String, conf: Session) -> Session {
	conf
}
