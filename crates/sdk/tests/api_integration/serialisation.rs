#![allow(clippy::unwrap_used)]

use surrealdb::opt::Config;
use surrealdb::types::SurrealValue;
use ulid::Ulid;

use super::CreateDb;

pub async fn serialise_uuid(new_db: impl CreateDb) {
	use uuid::Uuid;
	#[derive(Debug, SurrealValue)]
	struct Record {
		uuid: Uuid,
	}
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record = Record {
		uuid: Uuid::new_v4(),
	};
	let _: Option<Record> = db.create("user").content(record).await.unwrap();
}

define_include_tests!(serialisation => {

	#[test_log::test(tokio::test)]
	serialise_uuid,

});
