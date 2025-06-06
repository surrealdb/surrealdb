

pub mod google {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}

pub mod surrealdb {

    pub mod ast {
        include!(concat!(env!("OUT_DIR"), "/surrealdb.ast.rs"));
    }
    pub mod value {
        include!(concat!(env!("OUT_DIR"), "/surrealdb.value.rs"));
    }
}

mod value;
mod ast;

#[inline]
fn proto_timestamp_to_sql_datetime(
	proto: google::protobuf::Timestamp,
) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
	use chrono::TimeZone;

	let seconds = proto.seconds;
	let nanos = proto.nanos;

	// Convert to a DateTime<Utc>
	let datetime = chrono::Utc.timestamp_opt(seconds, nanos as u32);

	match datetime {
		chrono::LocalResult::Single(dt) => Ok(dt),
		_ => Err(anyhow::anyhow!("Invalid timestamp: seconds={}, nanos={}", seconds, nanos)),
	}
}


impl TryFrom<google::protobuf::Timestamp> for chrono::DateTime<chrono::Utc> {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(proto: google::protobuf::Timestamp) -> Result<Self, Self::Error> {
        proto_timestamp_to_sql_datetime(proto)
    }
}