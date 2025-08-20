use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct File {
	pub bucket: String,
	pub key: String,
}