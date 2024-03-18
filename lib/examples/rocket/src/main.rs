#[macro_use]
extern crate rocket;

use rocket_example::person;
use rocket_example::create_db_connection;


#[launch]
async fn rocket() -> _ {
	let db_conn = create_db_connection().await.unwrap();
	rocket::build()
		.mount(
			"/",
			routes![person::create, person::read, person::update, person::delete, person::list],
		)
		.manage(db_conn)
}
