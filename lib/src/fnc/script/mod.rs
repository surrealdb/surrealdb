#![cfg(feature = "scripting")]

pub use main::run;

mod classes;
mod error;
mod executor;
mod from;
mod globals;
mod into;
mod main;
mod modules;
