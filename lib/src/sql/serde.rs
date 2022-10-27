use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

thread_local! {
	static INTERNAL_SERIALIZATION: AtomicBool = AtomicBool::new(false);
}

/// *Advanced use only*. Enables a function to be run, whilst ensuring
/// that internal serialization is enabled for [`Value`](crate::sql::Value)
/// types. When using internal serialization the non-simplified
/// [`Value`](crate::sql::Value) type information is used, for
/// serialization to storage or for use in the binary WebSocket protocol.
pub fn serialize_internal<T, F: FnOnce() -> T>(f: F) -> T {
	beg_internal_serialization();
	let out = f();
	end_internal_serialization();
	out
}

/// *Advanced use only*. Checks if internal serialization is enabled for
/// [`Value`](crate::sql::Value) types. When using internal serialization
/// the non-simplified [`Value`](crate::sql::Value) type information is
/// used, for serialization to storage or for use in the binary WebSocket
/// protocol.
#[inline]
pub(crate) fn is_internal_serialization() -> bool {
	INTERNAL_SERIALIZATION.with(|v| v.load(Ordering::Relaxed))
}

/// *Advanced use only*. Marks the beginning of internal serialization for
/// [`Value`](crate::sql::Value) types. When using internal serialization
/// the non-simplified [`Value`](crate::sql::Value) type information is
/// used, for serialization to storage or for use in the binary WebSocket
/// protocol.
#[inline]
pub(crate) fn beg_internal_serialization() {
	INTERNAL_SERIALIZATION.with(|v| v.store(true, Ordering::Relaxed))
}

/// *Advanced use only*. Marks the end of internal serialization for
/// [`Value`](crate::sql::Value) types. When using internal serialization
/// the non-simplified [`Value`](crate::sql::Value) type information is
/// used, for serialization to storage or for use in the binary WebSocket
/// protocol.
#[inline]
pub(crate) fn end_internal_serialization() {
	INTERNAL_SERIALIZATION.with(|v| v.store(false, Ordering::Relaxed))
}
