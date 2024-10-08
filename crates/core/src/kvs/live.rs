use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Live {
	// TODO: optimisation this should probably be a &str
	/// The namespace in which this LIVE query exists
	pub ns: String,
	// TODO: optimisation this should probably be a &str
	/// The database in which this LIVE query exists
	pub db: String,
	// TODO: optimisation this should probably be a &str
	/// The table in which this LIVE query exists
	pub tb: String,
}
