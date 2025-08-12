use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::CreateDb;
use crate::api_integration::NS;

pub async fn serialise_uuid(new_db: impl CreateDb) {
	use uuid::Uuid;
	#[derive(Debug, Serialize, Deserialize)]
	struct Record {
		uuid: Uuid,
	}
	let (permit, db) = new_db.create_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
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
