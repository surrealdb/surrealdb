use rocket::create_db_connection;

#[macro_use]
extern crate rocket;

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
