#![cfg(feature = "scripting")]

pub use main::run;

mod classes;
mod error;
mod from;
mod globals;
mod into;
mod main;
mod modules;

#[cfg(feature = "http")]
mod fetch;
#[cfg(not(feature = "http"))]
mod fetch_stub;
#[cfg(not(feature = "http"))]
use self::fetch_stub as fetch;

#[cfg(test)]
mod tests;
