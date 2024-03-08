use crate::sql::value::Value;
use std::cmp::Ordering;
use std::mem;

#[derive(Default)]
// TODO Use surreal-kv once the number of record reach a given threshold
pub(super) struct StoreCollector(Vec<Value>);

impl StoreCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	// When surreal-kv will be used, the key will be used to sort the records in surreal-kv
	pub(super) fn sort_by<F>(&mut self, compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering,
	{
		self.0.sort_by(compare);
	}

	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	pub(super) fn start(&mut self, start: usize) {
		self.0 = mem::take(&mut self.0).into_iter().skip(start).collect();
	}
	pub(super) fn limit(&mut self, limit: usize) {
		self.0 = mem::take(&mut self.0).into_iter().take(limit).collect();
	}

	pub(super) fn take_vec(&mut self) -> Vec<Value> {
		mem::take(&mut self.0)
	}
	pub(super) fn take_store(&mut self) -> Self {
		Self(self.take_vec())
	}
}

impl<'a> IntoIterator for &'a mut StoreCollector {
	type Item = &'a mut Value;
	type IntoIter = std::slice::IterMut<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.iter_mut()
	}
}
