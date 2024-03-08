use crate::sql::value::Value;
use std::cmp::Ordering;
use std::mem;

#[derive(Default)]
pub(super) struct StoreCollector(Vec<Value>);

impl StoreCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	pub(super) fn sort_by<F>(&mut self, compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering,
	{
		self.0.sort_by(compare);
	}

	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	pub(super) fn skip(&mut self, start: usize) {
		self.0 = mem::take(&mut self.0).into_iter().skip(start).collect();
	}

	pub(super) fn clear(&mut self) {
		self.0.clear();
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
