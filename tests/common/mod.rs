#![allow(unused_imports)]
#![allow(dead_code)]

pub mod docker;
pub mod error;
pub mod expected;
pub mod format;
pub mod rest_client;
pub mod server;
pub mod socket;

pub use format::*;
pub use server::*;
pub use socket::*;

/// Check if the given message is a successful notification from LQ.
pub fn is_notification(msg: &serde_json::Value) -> bool {
	// Example of LQ notification:
	//
	// Object {"result": Object {"action": String("CREATE"), "id":
	// String("04460f07-b0e1-4339-92db-049a94aeec10"), "result": Object {"id":
	// String("table_FD40A9A361884C56B5908A934164884A:⟨an-id-goes-here⟩"), "name":
	// String("ok")}}}
	msg.is_object()
		&& msg["result"].is_object()
		&& msg["result"]
			.as_object()
			.unwrap()
			.keys()
			.all(|k| ["id", "action", "record", "result"].contains(&k.as_str()))
}

/// Check if the given message is a notification from LQ and comes from the
/// given LQ ID.
pub fn is_notification_from_lq(msg: &serde_json::Value, id: &str) -> bool {
	is_notification(msg)
		&& msg["result"].as_object().unwrap().get("id").unwrap().as_str() == Some(id)
}
