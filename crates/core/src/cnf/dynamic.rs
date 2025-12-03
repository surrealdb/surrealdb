use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use num_traits::ToPrimitive;

use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[derive(Default, Debug, Clone)]
pub(crate) struct DynamicConfiguration(Arc<Inner>);

#[derive(Default, Debug)]
struct Inner {
	query_timeout: AtomicU64,
}
impl DynamicConfiguration {
	/// Set a global query timeout for this Datastore
	pub(crate) fn set_query_timeout(&self, duration: Option<Duration>) {
		let val = match duration {
			None => 0,
			Some(d) => d.as_millis().to_u64().unwrap_or(u64::MAX),
		};
		self.0.query_timeout.store(val, Ordering::Relaxed);
	}

	pub(crate) fn get_query_timeout(&self) -> Option<Duration> {
		match self.0.query_timeout.load(Ordering::Relaxed) {
			0 => None,
			d => Some(Duration::from_millis(d)),
		}
	}
}

impl InfoStructure for DynamicConfiguration {
	fn structure(self) -> Value {
		let object = map! {
			"QUERY_TIMEOUT".to_string() => match self.get_query_timeout() {
				None => Value::None,
				Some(d) => d.into(),
			}
		};
		Value::Object(object.into())
	}
}
