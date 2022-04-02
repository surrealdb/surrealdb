pub use self::document::*;

#[cfg(feature = "parallel")]
mod compute;

mod admit;
mod allow;
mod check;
mod create;
mod delete;
mod document;
mod empty;
mod erase;
mod event;
mod exist;
mod grant;
mod index;
mod insert;
mod lives;
mod merge;
mod perms;
mod pluck;
mod purge;
mod relate;
mod select;
mod store;
mod table;
mod update;
