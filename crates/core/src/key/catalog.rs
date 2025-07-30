

pub(crate) mod ns {
    use crate::kvs::impl_key;
    use serde::{Deserialize, Serialize}; 

    #[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[non_exhaustive]
    pub struct CatalogNamespaceKey<'a> {
        __: u8,
        _a: u8,
        pub ns: &'a str,
    }
    impl_key!(CatalogNamespaceKey<'a>);

    pub fn new<'a>(ns: &'a str) -> CatalogNamespaceKey<'a> {
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
    use crate::kvs::impl_key;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[non_exhaustive]
    pub struct CatalogDatabaseKey<'a> {
        __: u8,
        _a: u8,
        pub ns: &'a str,
        _b: u8,
        pub db: &'a str,
    }
    impl_key!(CatalogDatabaseKey<'a>);

    pub fn new<'a>(ns: &'a str, db: &'a str) -> CatalogDatabaseKey<'a> {
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