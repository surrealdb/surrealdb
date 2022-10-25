use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

thread_local! {
	static INTERNAL_SERIALIZATION: AtomicBool = AtomicBool::new(false);
}

pub(crate) fn is_internal_serialization() -> bool {
	INTERNAL_SERIALIZATION.with(|v| v.load(Ordering::Relaxed))
}

pub fn beg_internal_serialization() {
	INTERNAL_SERIALIZATION.with(|v| v.store(true, Ordering::Relaxed))
}

pub fn end_internal_serialization() {
	INTERNAL_SERIALIZATION.with(|v| v.store(false, Ordering::Relaxed))
}
