use std::borrow::Borrow;
use std::collections::btree_map::{IntoIter as BIntoIter, Iter as BIter, IterMut as BIterMut};
use std::iter::FusedIterator;

use super::Value;
use crate::core::val;

transparent_wrapper! {
	#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
	pub struct Object(val::Object)
}
impl_serialize_wrapper!(Object);

impl Object {
	pub fn new() -> Self {
		Object(val::Object::default())
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
		self.0.remove_entry(key).map(|(a, b)| (a, Value::from_inner(b)))
	}

	pub fn iter(&self) -> Iter<'_> {
		Iter {
			iter: self.0.iter(),
		}
	}

	pub fn iter_mut(&mut self) -> IterMut<'_> {
		IterMut {
			iter: self.0.iter_mut(),
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
	iter: BIntoIter<String, val::Value>,
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
			iter: self.0.0.into_iter(),
		}
	}
}

#[derive(Clone)]
pub struct Iter<'a> {
	iter: BIter<'a, String, val::Value>,
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
		self.iter.next().map(|(a, b)| (a, Value::from_inner_ref(b)))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}

	fn last(self) -> Option<Self::Item>
	where
		Self: Sized,
	{
		self.iter.last().map(|(a, b)| (a, Value::from_inner_ref(b)))
	}

	fn min(mut self) -> Option<Self::Item> {
		self.iter.next().map(|(a, b)| (a, Value::from_inner_ref(b)))
	}

	fn max(mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|(a, b)| (a, Value::from_inner_ref(b)))
	}
}

impl FusedIterator for Iter<'_> {}

impl DoubleEndedIterator for Iter<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|(a, b)| (a, Value::from_inner_ref(b)))
	}
}

impl ExactSizeIterator for Iter<'_> {
	fn len(&self) -> usize {
		self.iter.len()
	}
}

pub struct IterMut<'a> {
	iter: BIterMut<'a, String, val::Value>,
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
		self.iter.next().map(|(a, b)| (a, Value::from_inner_mut(b)))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}

	fn last(self) -> Option<Self::Item>
	where
		Self: Sized,
	{
		self.iter.last().map(|(a, b)| (a, Value::from_inner_mut(b)))
	}

	fn min(mut self) -> Option<Self::Item> {
		self.iter.next().map(|(a, b)| (a, Value::from_inner_mut(b)))
	}

	fn max(mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|(a, b)| (a, Value::from_inner_mut(b)))
	}
}

impl FusedIterator for IterMut<'_> {}

impl DoubleEndedIterator for IterMut<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back().map(|(a, b)| (a, Value::from_inner_mut(b)))
	}
}

impl ExactSizeIterator for IterMut<'_> {
	fn len(&self) -> usize {
		self.iter.len()
	}
}
