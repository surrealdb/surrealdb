//! Plan-time folding: computing a value while planning, when the value is a constant.
//!
//! A fold runs in a restricted host that reads no data. [`evaluate`] holds the folder.

pub(crate) mod evaluate;

#[cfg(test)]
mod tests;
