

#[allow(warnings)]
#[rustfmt::skip]
pub mod flatbuffers {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/mod.rs"));
}

// Cap'n Proto
pub mod expr_capnp {
    include!(concat!(env!("OUT_DIR"), "/protocol/expr_capnp.rs"));
}

pub mod rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/protocol/rpc_capnp.rs"));
}


// Protobuf
pub mod google {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}

pub mod surrealdb {
    pub mod ast {
        include!(concat!(env!("OUT_DIR"), "/surrealdb.ast.rs"));
    }

    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/surrealdb.rpc.rs"));
    }

    pub mod value {
        include!(concat!(env!("OUT_DIR"), "/surrealdb.value.rs"));
    }
}

mod ast;
mod expr;
mod rpc;
mod value;

/// Traits 
pub trait ToCapnp {
    type Builder<'a>;

    fn to_capnp(&self, builder: Self::Builder<'_>);
}

pub trait FromCapnp {
    type Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self>
    where
        Self: Sized;
}

pub trait ToFlatbuffers {
    type Output<'bldr>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut ::flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr>;
}

pub trait FromFlatbuffers {
    type Input<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
    where
        Self: Sized;
}













// PROTOBUF STUFF TO DELETE SOON.

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