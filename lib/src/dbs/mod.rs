mod auth;
mod executor;
mod iterate;
mod iterator;
mod options;
mod response;
mod session;
mod statement;
mod transaction;
mod variables;

pub use self::auth::*;
pub use self::options::*;
pub use self::response::*;
pub use self::session::*;

pub(crate) use self::executor::*;
pub(crate) use self::iterator::*;
pub(crate) use self::statement::*;
pub(crate) use self::transaction::*;
pub(crate) use self::variables::*;

#[cfg(not(target_arch = "wasm32"))]
mod channel;

#[cfg(not(target_arch = "wasm32"))]
pub use self::channel::*;

#[cfg(test)]
pub(crate) mod test;

pub(crate) const LOG: &str = "surrealdb::dbs";
