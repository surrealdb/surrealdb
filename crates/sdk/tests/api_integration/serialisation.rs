use std::collections::BTreeMap;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use surrealdb::expr::Value;
use surrealdb_protocol::TryFromValue;
use surrealdb_protocol::proto::v1::Value as ValueProto;
use ulid::Ulid;

use crate::api_integration::NS;

use super::CreateDb;

pub async fn serialise_uuid(new_db: impl CreateDb) {
	use uuid::Uuid;
	#[derive(Debug, Serialize, Deserialize)]
	struct Record {
		uuid: Uuid,
	}

	impl TryInto<Value> for Record {
		type Error = anyhow::Error;
		fn try_into(self) -> Result<Value, Self::Error> {
			Ok(Value::from(BTreeMap::from([("uuid".to_string(), Value::Uuid(self.uuid.into()))])))
		}
	}

	impl TryFromValue for Record {
		fn try_from_value(mut value: ValueProto) -> anyhow::Result<Self> {
			let uuid = Uuid::try_from_value(value.remove("uuid").context("Expected uuid")?)?;
			Ok(Self {
				uuid,
			})
		}
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
