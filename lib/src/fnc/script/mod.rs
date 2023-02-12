#![cfg(feature = "scripting")]

const LOG: &str = "surrealdb::jsr";

pub use main::run;

mod classes;
mod error;
mod executor;
mod from;
mod globals;
mod into;
mod main;
mod modules;
