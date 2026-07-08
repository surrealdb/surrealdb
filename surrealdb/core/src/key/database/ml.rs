//! Stores a DEFINE MODEL config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::MlModelDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, )]
	pub(crate) struct Ml<'a> {
		pub prefix: DatabaseRoot,
			b'!',
			b'm',
			b'l',
		pub ml: Cow<'a, str>,
		pub vn: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(Ml<'a> => MlModelDefinition);
impl Categorise for Ml<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseModel
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, )]
	pub(crate) struct MlPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'm',
		b'l',
	}
}
impl_kv_range_storekey!(MlPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = Ml {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ml: "testml".into(),
			vn: "1.0.0".into(),
		};
		let enc = Ml::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mltestml\x001.0.0\0");
	}

	#[test]
	fn test_prefix() {
		let val = MlPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		}
		.encode_bound()
		.unwrap();
		assert_eq!(val.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ml");
	}
}
