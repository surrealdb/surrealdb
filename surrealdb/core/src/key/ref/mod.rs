//! key!{Stores a graph edge pointer
use std::borrow::Cow;

use anyhow::Result;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::{RecordIdKey, TableName};

key! {
	#[derive(Clone, Debug, Eq, PartialEq)]
	pub(crate) struct Prefix<'a> {
		pub root: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'&',
		pub id: Cow<'a,RecordIdKey>,
	}
}

impl_kv_range_storekey!(Prefix<'_>);

key! {
	/// A table-level prefix covering every reference key whose *target* record
	/// lives in `tb`, across all target record ids. Used by `REMOVE FIELD` to find
	/// and purge the reference keys a removed reference field wrote (which are
	/// keyed by the target record, not by the referencing field).
	#[derive(Clone, Debug, Eq, PartialEq)]
	pub struct PrefixTb<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'&',
	}
}

impl_kv_range_storekey!(PrefixTb<'_>);

key! {
	#[derive(Clone, Debug, Eq, PartialEq)]
	pub(crate) struct PrefixFt<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'&',
		pub id: Cow<'a,RecordIdKey>,
		pub ft: Cow<'a, str>,
	}
}

impl_kv_range_storekey!(PrefixFt<'_>);

key! {
	#[derive(Clone, Debug, Eq, PartialEq)]
	pub(crate) struct PrefixField<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'&',
		pub id: Cow<'a,RecordIdKey>,
		pub ft: Cow<'a, str>,
		pub ff: Cow<'a, str>,
	}
}

impl_kv_range_storekey!(PrefixField<'_>);

// The order in this key is made so we can scan:
// - all references for a given record
// - all references for a given record, filtered by a origin table
// - all references for a given record, filtered by a origin table and an origin field
key! {
	#[derive(Clone, Debug, Eq, PartialEq)]
	pub(crate) struct Ref<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub table: Cow<'a, TableName>,
		b'&',
		pub id: Cow<'a, RecordIdKey>,
		pub foreign_table: Cow<'a, TableName>,
		pub foreign_field: Cow<'a, str>,
		pub foreign_key: Cow<'a, RecordIdKey>,
	}
}

impl_kv_key_storekey!(Ref<'a> => ());

impl Categorise for Ref<'_> {
	fn categorise(&self) -> Category {
		Category::Ref
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let binding = RecordIdKey::String(Strand::new_static("testid"));
		let other_binding = RecordIdKey::String(Strand::new_static("otherid"));
		let tb: TableName = "testtb".into();
		let ft: TableName = "othertb".into();
		let val = Ref {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			table: Cow::Borrowed(&tb),
			id: Cow::Borrowed(&binding),
			foreign_table: Cow::Borrowed(&ft),
			foreign_field: Cow::Borrowed("test.*"),
			foreign_key: Cow::Borrowed(&other_binding),
		};
		let enc = Ref::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00&\x03testid\0othertb\0test.*\0\x03otherid\0"
		);
	}

	#[test]
	fn prefix_tb_bounds_table_refs() {
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let fk = RecordIdKey::String(Strand::new_static("otherid"));
		let tb_a: TableName = "aaa".into();
		let tb_b: TableName = "bbb".into();
		let ft: TableName = "ref_from".into();

		let enc_a = Ref::encode_key(&Ref {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			table: Cow::Borrowed(&tb_a),
			id: Cow::Borrowed(&id),
			foreign_table: Cow::Borrowed(&ft),
			foreign_field: Cow::Borrowed("field"),
			foreign_key: Cow::Borrowed(&fk),
		})
		.unwrap();
		let enc_b = Ref::encode_key(&Ref {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			table: Cow::Borrowed(&tb_b),
			id: Cow::Borrowed(&id),
			foreign_table: Cow::Borrowed(&ft),
			foreign_field: Cow::Borrowed("field"),
			foreign_key: Cow::Borrowed(&fk),
		})
		.unwrap();

		let range = PrefixTb {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb_a),
		}
		.encode_range()
		.unwrap();

		// A reference key whose target record is in `aaa` sorts within `aaa`'s
		// table-level range, so a range scan of [beg, end) finds it...
		assert!(
			range.start.as_slice() < enc_a.as_slice() && enc_a.as_slice() < range.end.as_slice()
		);
		// ...while a key whose target is in another table does not.
		assert!(
			!(range.start.as_slice() < enc_b.as_slice() && enc_b.as_slice() < range.end.as_slice())
		);
	}
}
