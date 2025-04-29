#[cfg(not(feature = "enterprise"))]
pub(crate) mod backend;
#[cfg(feature = "enterprise")]
pub(crate) use crate::ent::buc::backend;
mod connection;
pub(crate) use connection::{connect, connect_global, BucketConnectionKey, BucketConnections};
mod controller;
pub(crate) use controller::BucketController;
pub use controller::BucketOperation;

pub(crate) mod store;
