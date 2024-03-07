use crate::sql::value::Value;
use std::cmp::Ordering;
use std::mem;

pub(super) enum Collector {
	Count(CountCollector),
	Memory(MemoryCollector),
}

impl Default for Collector {
	fn default() -> Self {
		Self::Memory(MemoryCollector::default())
	}
}

pub(super) trait CollectorAPI {
	fn new_instance(&self) -> Self;
	fn push(&mut self, val: Value);
	fn sort_by<F>(&mut self, compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering;
	fn len(&self) -> usize;

	fn skip(&mut self, start: usize);
	fn clear(&mut self);

	fn take(&mut self) -> Vec<Value>;
}

impl<'a> IntoIterator for &'a mut Collector {
	type Item = &'a mut Value;
	type IntoIter = std::slice::IterMut<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		match self {
			Collector::Count(c) => c.into_iter(),
			Collector::Memory(c) => c.into_iter(),
		}
	}
}

impl CollectorAPI for Collector {
	fn new_instance(&self) -> Self {
		match self {
			Collector::Count(c) => Collector::Count(c.new_instance()),
			Collector::Memory(c) => Collector::Memory(c.new_instance()),
		}
	}

	fn push(&mut self, val: Value) {
		match self {
			Collector::Count(c) => c.push(val),
			Collector::Memory(c) => c.push(val),
		}
	}

	fn sort_by<F>(&mut self, compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering,
	{
		match self {
			Collector::Count(c) => c.sort_by(compare),
			Collector::Memory(c) => c.sort_by(compare),
		}
	}

	fn len(&self) -> usize {
		match self {
			Collector::Count(c) => c.len(),
			Collector::Memory(c) => c.len(),
		}
	}

	fn skip(&mut self, start: usize) {
		match self {
			Collector::Count(c) => c.skip(start),
			Collector::Memory(c) => c.skip(start),
		}
	}

	fn clear(&mut self) {
		match self {
			Collector::Count(c) => c.clear(),
			Collector::Memory(c) => c.clear(),
		}
	}

	fn take(&mut self) -> Vec<Value> {
		match self {
			Collector::Count(c) => c.take(),
			Collector::Memory(c) => c.take(),
		}
	}
}

#[derive(Default)]
struct MemoryCollector(Vec<Value>);

impl CollectorAPI for MemoryCollector {
	fn new_instance(&self) -> Self {
		Self::default()
	}

	fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	fn sort_by<F>(&mut self, compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering,
	{
		self.0.sort_by(compare);
	}

	fn len(&self) -> usize {
		self.0.len()
	}

	fn skip(&mut self, start: usize) {
		self.0 = mem::take(&mut self.0).into_iter().skip(start).collect();
	}

	fn clear(&mut self) {
		self.0.clear();
	}

	fn take(&mut self) -> Vec<Value> {
		mem::take(&mut self.0)
	}
}

#[derive(Default)]
struct CountCollector(usize);

impl CollectorAPI for CountCollector {
	fn new_instance(&self) -> Self {
		Self::default()
	}

	fn push(&mut self, _val: Value) {
		self.0 += 1;
	}

	fn sort_by<F>(&mut self, _compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering,
	{
		// Nothing to do, it is an aggregation
	}

	fn len(&self) -> usize {
		self.0
	}

	fn skip(&mut self, _start: usize) {
		// Nothing to do, it is an aggregation
	}

	fn clear(&mut self) {
		self.0 = 0;
	}

	fn take(&mut self) -> Vec<Value> {
		vec![self.0.into()]
	}
}

impl<'a> IntoIterator for &'a mut CountCollector {
	type Item = &'a mut Value;
	type IntoIter = std::slice::IterMut<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		unreachable!()
	}
}

impl<'a> IntoIterator for &'a mut MemoryCollector {
	type Item = &'a mut Value;
	type IntoIter = std::slice::IterMut<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.iter_mut()
	}
}
