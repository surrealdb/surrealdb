use crate::{catalog::{DatabaseId, NamespaceId}, kvs::impl_kv_value_revisioned};
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Live {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: String,
}
impl_kv_value_revisioned!(Live);
