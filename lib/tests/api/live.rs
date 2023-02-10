// Live Query tests
// These require a web socket connection due to the open-channel nature of processing new events

#[tokio::test]
async fn open_live_query() {
	// init(4);

	let unique = Ulid::new().to_string();
	let table_name = format!("OpenLiveQueryTable{unique}");
	let person1_id = format!("{table_name}:person1");

	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let create_res = db.query(format!("CREATE {person1_id} SET name='person 1'")).await;

	let lq_res = db.live(table_name.clone()).await;
	let update_res = db
		.query(format!("UPDATE {table_name} SET name='another person 1' WHERE id={person1_id}"))
		.await;

	println!("Create response was {create_res:?} and live query response was {lq_res:?}");
	println!("Update response was {update_res:?}");
	panic!("magicword test failure")
}
