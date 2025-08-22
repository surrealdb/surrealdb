pub(crate) mod ns {
	use serde::{Deserialize, Serialize};

	use crate::catalog::NamespaceDefinition;
	use crate::kvs::KVKey;

	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
	#[non_exhaustive]
	pub(crate) struct CatalogNamespaceKey<'a> {
		__: u8,
		_a: u8,
		pub ns: &'a str,
	}

	impl KVKey for CatalogNamespaceKey<'_> {
		type ValueType = NamespaceDefinition;
	}

	pub(crate) fn new(ns: &str) -> CatalogNamespaceKey<'_> {
		CatalogNamespaceKey::new(ns)
	}

	impl<'a> CatalogNamespaceKey<'a> {
		pub fn new(ns: &'a str) -> Self {
			Self {
				__: b'/',
				_a: b'?',
				ns,
			}
		}
	}
}

pub(crate) mod db {
	use serde::{Deserialize, Serialize};

	use crate::catalog::DatabaseDefinition;
	use crate::kvs::KVKey;

	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
	#[non_exhaustive]
	pub(crate) struct CatalogDatabaseKey<'a> {
		__: u8,
		_a: u8,
		pub ns: &'a str,
		_b: u8,
		pub db: &'a str,
	}
	impl KVKey for CatalogDatabaseKey<'_> {
		type ValueType = DatabaseDefinition;
	}

	pub(crate) fn new<'a>(ns: &'a str, db: &'a str) -> CatalogDatabaseKey<'a> {
		CatalogDatabaseKey::new(ns, db)
	}

	impl<'a> CatalogDatabaseKey<'a> {
		pub fn new(ns: &'a str, db: &'a str) -> Self {
			Self {
				__: b'/',
				_a: b'?',
				ns,
				_b: b'*',
				db,
			}
		}
	}
}

pub(crate) mod tb {
	use serde::{Deserialize, Serialize};

	use crate::catalog::TableDefinition;
	use crate::kvs::KVKey;

	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
	#[non_exhaustive]
	pub(crate) struct CatalogTableKey<'a> {
		__: u8,
		_a: u8,
		pub ns: &'a str,
		_b: u8,
		pub db: &'a str,
		_c: u8,
		pub tb: &'a str,
	}
	impl KVKey for CatalogTableKey<'_> {
		type ValueType = TableDefinition;
	}

	pub(crate) fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str) -> CatalogTableKey<'a> {
		CatalogTableKey::new(ns, db, tb)
	}

	impl<'a> CatalogTableKey<'a> {
		pub fn new(ns: &'a str, db: &'a str, tb: &'a str) -> Self {
			Self {
				__: b'/',
				_a: b'?',
				ns,
				_b: b'*',
				db,
				_c: b'*',
				tb,
			}
		}
	}
}
