#[test_log::test(tokio::test)]
async fn serialise_uuid() {
	use uuid::Uuid;
	#[derive(Debug, Serialize, Deserialize)]
	struct Record {
		uuid: Uuid,
	}
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record = Record {
		uuid: Uuid::new_v4(),
	};
	let _: Option<Record> = db.create("user").content(record).await.unwrap();
}
