use crate::web::head;
use serde::Deserialize;
use warp::path;
use warp::Filter;

#[derive(Default, Deserialize, Debug, Clone)]
pub struct Query {
	pub limit: Option<String>,
	pub start: Option<String>,
}

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	let base = warp::path("key");
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);

	// ------------------------------
	// Routes for a table
	// ------------------------------

	// Set base path for all
	let base = path!("key" / String).and(warp::path::end());
	// Set select method
	let select = base.and(warp::get()).and(warp::query()).and_then(select_all);
	// Set create method
	let create = base
		.and(warp::post())
		.and(warp::body::content_length_limit(1024 * 1)) // 1MiB
		.and(warp::body::bytes())
		.and_then(create_all);
	// Set delete method
	let delete = base.and(warp::delete()).and_then(delete_all);
	// Specify route
	let all = select.or(create).or(delete);

	// ------------------------------
	// Routes for a thing
	// ------------------------------

	// Set base path for one
	let base = path!("key" / String / String).and(warp::path::end());
	// Set select method
	let select = base.and(warp::get()).and_then(select_one);
	// Set create method
	let create = base
		.and(warp::post())
		.and(warp::body::content_length_limit(1024 * 1)) // 1MiB
		.and(warp::body::bytes())
		.and_then(create_one);
	// Set update method
	let update = base
		.and(warp::put())
		.and(warp::body::content_length_limit(1024 * 1)) // 1MiB
		.and(warp::body::bytes())
		.and_then(update_one);
	// Set modify method
	let modify = base
		.and(warp::patch())
		.and(warp::body::content_length_limit(1024 * 1)) // 1MiB
		.and(warp::body::bytes())
		.and_then(modify_one);
	// Set delete method
	let delete = base.and(warp::delete()).and_then(delete_one);
	// Specify route
	let one = select.or(create).or(update).or(modify).or(delete);

	// ------------------------------
	// All routes
	// ------------------------------

	// Specify route
	opts.or(all).or(one).with(head::cors())
}

// ------------------------------
// Routes for a table
// ------------------------------

async fn select_all(table: String, query: Query) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!(
		"SELECT * FROM type::table('{t}') LIMIT {l} START {s}",
		t = table,
		l = query.limit.unwrap_or(String::from("100")),
		s = query.start.unwrap_or(String::from("0")),
	);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

async fn create_all(
	table: String,
	body: bytes::Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("CREATE type::table('{t}') CONTENT $data", t = table);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

async fn delete_all(table: String) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("DELETE type::table('{t}')", t = table);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

// ------------------------------
// Routes for a thing
// ------------------------------

async fn select_one(table: String, id: String) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("SELECT * FROM type::thing('{t}', '{i}')", t = table, i = id);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

async fn create_one(
	table: String,
	id: String,
	body: bytes::Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("UPDATE type::thing('{t}', '{i}') CONTENT $data", t = table, i = id);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

async fn update_one(
	table: String,
	id: String,
	body: bytes::Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("UPDATE type::thing('{t}', '{i}') CONTENT $data", t = table, i = id);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

async fn modify_one(
	table: String,
	id: String,
	body: bytes::Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("UPDATE type::thing('{t}', '{i}') MERGE $data", t = table, i = id);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}

async fn delete_one(table: String, id: String) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = format!("DELETE type::thing('{t}', '{i}')", t = table, i = id);
	Ok(warp::reply::with_status(sql, http::StatusCode::OK))
}
