use rocket::http::{ContentType, Status};
use rocket::local::asynchronous::Client;
use rocket_example::create_db_connection;
use rocket_example::person::Person;
use rocket_example::router;

macro_rules! run_test {
	(|$client:ident| $block:expr) => {{
		rocket::async_test(async move {
			let db_conn = create_db_connection().await.unwrap();
			let $client = Client::tracked(router(db_conn)).await.unwrap();
			$block
		});
	}};
}

async fn query_persons(client: &Client) -> Vec<Person> {
	let response = client.get("/people").dispatch().await;
	assert_eq!(response.status(), Status::Ok);
	let body = response.into_string().await.unwrap();
	serde_json::from_str(&body).unwrap()
}

async fn read_person(client: &Client, id: i32) -> Person {
	let response = client.get(format!("/person/{}", id)).dispatch().await;
	assert_eq!(response.status(), Status::Ok);
	let body = response.into_string().await.unwrap();
	println!("{body}");
	serde_json::from_str(&body).unwrap()
}

async fn update_person(client: &Client, id: i32, new_name: &str) {
	let response = client
		.put(format!("/person/{}", id))
		.header(ContentType::JSON)
		.body(format!(r#"{{"name":"{}"}}"#, new_name))
		.dispatch()
		.await;
	assert_eq!(response.status(), Status::Ok);
}

async fn delete_person(client: &Client, id: i32) {
	let response = client.delete(format!("/person/{}", id)).dispatch().await;
	assert_eq!(response.status(), Status::Ok);
}

async fn delete_all_persons(client: &Client) {
	let response = client.delete("/persons").dispatch().await;
	assert_eq!(response.status(), Status::Ok);
}

#[test]
fn test_read_person() {
	run_test!(|client| {
		// Insert a person to ensure the database is not empty.
		let john_id = 1;
		client
			.post(format!("/person/{}", john_id))
			.header(ContentType::JSON)
			.body(r#"{"name":"John Doe"}"#)
			.dispatch()
			.await;

		// Read the inserted person's data.
		let person_response = client.get(format!("/person/{}", john_id)).dispatch().await;
		assert_eq!(person_response.status(), Status::Ok);
		let person_data: Option<Person> =
			serde_json::from_str(&person_response.into_string().await.unwrap()).unwrap();

		// Verify that the data matches what was inserted.
		assert!(person_data.is_some());
		let person = person_data.unwrap();
		assert_eq!(person.name, "John Doe");

		// Cleanup
		delete_person(&client, john_id).await;
	});
}

#[test]
fn test_update_person() {
	run_test!(|client| {
		let john_id = 1;

		// Update John Doe to Jane Doe
		update_person(&client, john_id, "Jane Doe").await;

		// Verify update
		let updated_person = read_person(&client, john_id).await;
		assert_eq!(updated_person.name, "Jane Doe");

		// Cleanup
		delete_person(&client, john_id).await;
	});
}

#[test]
fn test_insertion_deletion() {
	run_test!(|client| {
		// Get the person before making changes.
		let init_persons = query_persons(&client).await;
		let john_id = 1;
		// Issue a request to insert a new person.
		client
			.post(format!("/person/{}", john_id))
			.header(ContentType::JSON)
			.body(r#"{"name":"John Doe"}"#)
			.dispatch()
			.await;

		// Ensure we have one more person in the database.
		let new_persons = query_persons(&client).await;
		assert_eq!(new_persons.len(), init_persons.len() + 1);

		// Ensure the person is what we expect.
		let john = &new_persons[0];
		assert_eq!(john.name, "John Doe");

		// Issue a request to delete the person.
		delete_person(&client, john_id).await;

		// Ensure it's gone.
		let final_persons = query_persons(&client).await;
		assert_eq!(final_persons.len(), init_persons.len());
		if !final_persons.is_empty() {
			assert_ne!(final_persons[0].name, "John Doe");
		}

		delete_all_persons(&client).await;
	});
}
