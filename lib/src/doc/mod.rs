pub(crate) use self::document::*;

#[cfg(feature = "parallel")]
mod compute;

mod allow;
mod alter;
mod check;
mod clean;
mod create;
mod delete;
mod document;
mod edges;
mod empty;
mod erase;
mod event;
mod exist;
mod field;
mod index;
mod insert;
mod lives;
mod merge;
mod pluck;
mod purge;
mod relate;
mod select;
mod store;
mod table;
mod update;
