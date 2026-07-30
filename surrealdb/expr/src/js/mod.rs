//! JavaScript (QuickJS) conversions for the value model.
//!
//! Implements the `js` engine's conversion traits for [`Value`](crate::val::Value)
//! and hosts the value-backed JS classes (Record, Uuid, Duration, File).

pub mod classes;

mod from;
mod into;
