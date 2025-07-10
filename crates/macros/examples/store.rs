// cargo expand --example key

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_macros::Store;

mod err {
	#[derive(Debug)]
	pub struct Error;

	impl From<revision::Error> for Error {
		fn from(_: revision::Error) -> Self {
			unimplemented!();
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Store)]
struct Record {
	id: u64,
	name: String,
}

fn main() {}
