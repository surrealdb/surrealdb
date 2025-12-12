pub use surrealdb_types::Tokens;
use surrealdb_types::{object, Variables};

pub(crate) struct AuthParams {
    pub ns: Option<String>,
    pub db: Option<String>,
    pub ac: Option<String>,
    pub vars: Variables,
}

impl AuthParams {
    pub fn into_vars(self) -> Variables {
        let mut vars = self.vars;
        if let Some(ns) = self.ns {
            vars.insert("ns".to_string(), ns);
        }
        if let Some(db) = self.db {
            vars.insert("db".to_string(), db);
        }
        if let Some(ac) = self.ac {
            vars.insert("ac".to_string(), ac);
        }

        vars
    }
}

pub struct RootUser {
    pub access: Option<String>,
    pub username: String,
    pub password: String,
}

impl From<RootUser> for AuthParams {
    fn from(root: RootUser) -> Self {
        Self {
            ns: None,
            db: None,
            ac: root.access,
            vars: Variables::from(object! {
                user: root.username,
                pass: root.password,
            }),
        }
    }
}

pub struct NamespaceUser {
    pub namespace: String,
    pub access: Option<String>,
    pub username: String,
    pub password: String,
}

impl From<NamespaceUser> for AuthParams {
    fn from(namespace: NamespaceUser) -> Self {
        Self {
            ns: Some(namespace.namespace),
            db: None,
            ac: namespace.access,
            vars: Variables::from(object! {
                user: namespace.username,
                pass: namespace.password,
            }),
        }
    }
}

pub struct DatabaseUser {
    pub namespace: String,
    pub database: String,
    pub access: Option<String>,
    pub username: String,
    pub password: String,
}

impl From<DatabaseUser> for AuthParams {
    fn from(database: DatabaseUser) -> Self {
        Self {
            ns: Some(database.namespace),
            db: Some(database.database),
            ac: database.access,
            vars: Variables::from(object! {
                user: database.username,
                pass: database.password,
            }),
        }
    }
}

pub struct AccessBearerAuth {
    pub namespace: Option<String>,
    pub database: Option<String>,
    pub access: String,
    pub key: String,
}

impl AccessBearerAuth {
    pub fn root(access: String, key: String) -> Self {
        Self {
            namespace: None,
            database: None,
            access,
            key,
        }
    }

    pub fn namespace(namespace: String, access: String, key: String) -> Self {
        Self {
            namespace: Some(namespace),
            database: None,
            access,
            key,
        }
    }
    
    pub fn database(namespace: String, database: String, access: String, key: String) -> Self {
        Self {
            namespace: Some(namespace),
            database: Some(database),
            access,
            key,
        }
    }
}

impl From<AccessBearerAuth> for AuthParams {
    fn from(auth: AccessBearerAuth) -> Self {
        Self {
            ns: auth.namespace,
            db: auth.database,
            ac: Some(auth.access),
            vars: Variables::from(object! {
                key: auth.key,
            }),
        }
    }
}

pub struct AccessRecordAuth {
    pub namespace: String,
    pub database: String,
    pub access: String,
    pub params: Variables,
}

impl From<AccessRecordAuth> for AuthParams {
    fn from(auth: AccessRecordAuth) -> Self {
        Self {
            ns: Some(auth.namespace),
            db: Some(auth.database),
            ac: Some(auth.access),
            vars: auth.params,
        }
    }
}

pub struct AccessToken(String);
impl AccessToken {
    pub fn new<T: Into<String>>(token: T) -> Self {
        Self(token.into())
    }
}

impl<T: Into<String>> From<T> for AccessToken {
    fn from(token: T) -> Self {
        Self(token.into())
    }
}

impl From<AccessToken> for Tokens {
    fn from(token: AccessToken) -> Self {
        Self {
            access: Some(token.0),
            refresh: None,
        }
    }
}

pub struct RefreshToken(String);
impl RefreshToken {
    pub fn new<T: Into<String>>(token: T) -> Self {
        Self(token.into())
    }
}

impl<T: Into<String>> From<T> for RefreshToken {
    fn from(token: T) -> Self {
        Self(token.into())
    }
}

impl From<RefreshToken> for Tokens {
    fn from(token: RefreshToken) -> Self {
        Self {
            access: None,
            refresh: Some(token.0),
        }
    }
}