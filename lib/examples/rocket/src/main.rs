#[macro_use]
extern crate rocket;

use rocket_example::{create_db_connection, router};

#[launch]
pub async fn rocket() -> _ {
	let db_conn = create_db_connection().await.unwrap();
	router(db_conn)
}
