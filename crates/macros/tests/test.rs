mod err {
	#[derive(Debug)]
	pub struct Error;

	impl From<revision::Error> for Error {
		fn from(_: revision::Error) -> Self {
			unimplemented!();
		}
	}
}
mod test {
	use revision::revisioned;
	use serde::{Deserialize, Serialize};
	use surrealdb_macros::Store;

	#[revisioned(revision = 1)]
	#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Store)]
	struct Record {
		id: u64,
		name: String,
	}

	#[test]
	fn store_try_from() {
		let ser = Record {
			id: 0,
			name: "test".to_string(),
		};
		let buf: Vec<u8> = ser.clone().try_into().unwrap();
		let deser: Record = buf.try_into().unwrap();
		assert_eq!(ser, deser);
	}

	#[test]
	fn store_ref_try_from() {
		let ser = Record {
			id: 0,
			name: "test".to_string(),
		};
		let buf: Vec<u8> = (&ser).try_into().unwrap();
		let deser: Record = buf.try_into().unwrap();
		assert_eq!(ser, deser);
	}
}
