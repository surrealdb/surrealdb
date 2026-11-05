//! The error every exported method reports.
//!
//! NAPI turns an `Err` into a thrown JavaScript value, so this is only ever a
//! message: the engine's errors already carry their own text, and the
//! JavaScript SDK reads a *method* failure off the RPC reply envelope rather
//! than from a throw.

pub fn err_map(err: impl std::fmt::Display) -> napi::Error {
	napi::Error::from_reason(err.to_string())
}
