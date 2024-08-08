use super::Value;
use std::{borrow::Borrow, collections::BTreeMap, iter::FusedIterator};
use surrealdb_core::sql::{Object as CoreObject, Value as CoreValue};

transparent_wrapper!(
	#[derive(Debug, Default, Clone,  PartialEq, PartialOrd)]
	pub struct Object(CoreObject)
);

impl Object {
	pub fn new() -> Self {
		Object(CoreObject::default())
	}

	pub fn clear(&mut self) {
		self.0.clear()
	}

	pub fn get<Q>(&self, key: &Q) -> Option<&Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get(key).map(Value::from_inner_ref)
	}

	pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get_mut(key).map(Value::from_inner_mut)
	}

	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		String: Borrow<Q>,
		Q: ?Sized + Ord,
	{
		self.0.contains_key(key)
	}

	pub fn remove<Q>(&mut self, key: &Q) -> Option<Value>
	where
		String: Borrow<Q>,
		Q: ?Sized + Ord,
	{
		self.0.remove(key).map(Value::from_inner)
	}

	pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(String, Value)>
	where
		String: Borrow<Q>,
		Q: ?Sized + Ord,
	{
		self.0.remove_entry(key).map(|x| (x.0, Value::from_inner(x.1)))
	}

	pub fn iter(&self) -> Iter<'_> {
		Iter {
			iter: self.0 .0.iter(),
		}
	}

	pub fn iter_mut(&mut self) -> IterMut<'_> {
		IterMut {
			iter: self.0 .0.iter_mut(),
		}
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn insert<V>(&mut self, key: String, value: V) -> Option<Value>
	where
		V: Into<Value>,
	{
		self.0.insert(key, value.into().into_inner()).map(Value::from_inner)
	}
}

pub struct IntoIter {
	iter: <BTreeMap<String, CoreValue> as IntoIterator>::IntoIter,
}

impl Iterator for IntoIter {
	type Item = (String, Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().map(|x| (x.0, Value::from_inner(x.1)))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}
}

impl DoubleEndedIterator for IntoIter {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|x| (x.0, Value::from_inner(x.1)))
	}
}

impl ExactSizeIterator for IntoIter {
	fn len(&self) -> usize {
		self.iter.len()
	}
}

impl FusedIterator for IntoIter {}

impl IntoIterator for Object {
	type Item = (String, Value);

	type IntoIter = IntoIter;

	fn into_iter(self) -> Self::IntoIter {
		IntoIter {
			iter: self.0.into_iter(),
		}
	}
}

#[derive(Clone)]
pub struct Iter<'a> {
	iter: <&'a BTreeMap<String, CoreValue> as IntoIterator>::IntoIter,
}

impl<'a> IntoIterator for &'a Object {
	type Item = (&'a String, &'a Value);

	type IntoIter = Iter<'a>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

impl<'a> Iterator for Iter<'a> {
	type Item = (&'a String, &'a Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().map(|x| (x.0, Value::from_inner_ref(x.1)))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}

	fn last(self) -> Option<Self::Item>
	where
		Self: Sized,
	{
		self.iter.last().map(|x| (x.0, Value::from_inner_ref(x.1)))
	}

	fn min(mut self) -> Option<Self::Item> {
		self.iter.next().map(|x| (x.0, Value::from_inner_ref(x.1)))
	}

	fn max(mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|x| (x.0, Value::from_inner_ref(x.1)))
	}
}

impl FusedIterator for Iter<'_> {}

impl<'a> DoubleEndedIterator for Iter<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|x| (x.0, Value::from_inner_ref(x.1)))
	}
}

impl<'a> ExactSizeIterator for Iter<'a> {
	fn len(&self) -> usize {
		self.iter.len()
	}
}

pub struct IterMut<'a> {
	iter: <&'a mut BTreeMap<String, CoreValue> as IntoIterator>::IntoIter,
}

impl<'a> IntoIterator for &'a mut Object {
	type Item = (&'a String, &'a mut Value);

	type IntoIter = IterMut<'a>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter_mut()
	}
}

impl<'a> Iterator for IterMut<'a> {
	type Item = (&'a String, &'a mut Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().map(|x| (x.0, Value::from_inner_mut(x.1)))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}

	fn last(self) -> Option<Self::Item>
	where
		Self: Sized,
	{
		self.iter.last().map(|x| (x.0, Value::from_inner_mut(x.1)))
	}

	fn min(mut self) -> Option<Self::Item> {
		self.iter.next().map(|x| (x.0, Value::from_inner_mut(x.1)))
	}

	fn max(mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|x| (x.0, Value::from_inner_mut(x.1)))
	}
}

impl FusedIterator for IterMut<'_> {}

impl<'a> DoubleEndedIterator for IterMut<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|x| (x.0, Value::from_inner_mut(x.1)))
	}
}

impl<'a> ExactSizeIterator for IterMut<'a> {
	fn len(&self) -> usize {
		self.iter.len()
	}
}
