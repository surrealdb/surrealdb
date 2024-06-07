use std::{
	borrow::Borrow,
	collections::{btree_map::IterMut, BTreeMap},
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub struct Bytes(Vec<u8>);

#[derive(Debug)]
pub struct Datetime(DateTime<Utc>);

#[derive(Debug)]
#[non_exhaustive]
pub enum RecordIdKey {
	String(String),
	Integer(i64),
	Object(Object),
	Array(Array),
}

#[derive(Debug)]
pub struct RecordId {
	table: String,
	key: RecordIdKey,
}

#[derive(Debug)]
pub enum Number {
	Float(f64),
	Number(i64),
	Decimal(Decimal),
}

#[derive(Debug)]
pub struct ObjectIterMut<'a>(IterMut<'a, String, Value>);

impl<'a> Iterator for ObjectIterMut<'a> {
	type Item = (&'a String, &'a mut Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
}

impl<'a> std::iter::FusedIterator for ObjectIterMut<'a> {}

impl<'a> ExactSizeIterator for ObjectIterMut<'a> {
	fn len(&self) -> usize {
		<IterMut<'a, String, Value> as ExactSizeIterator>::len(&self.0)
	}
}

#[derive(Debug)]
pub struct Object(BTreeMap<String, Value>);

impl Object {
	pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&Value>
	where
		String: Borrow<Q>,
	{
		self.0.get(key)
	}

	pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Value>
	where
		String: Borrow<Q>,
	{
		self.0.get_mut(key)
	}

	pub fn iter_mut(&mut self) -> ObjectIterMut<'_> {
		ObjectIterMut(self.0.iter_mut())
	}
}

#[derive(Debug)]
pub struct Array(Vec<Value>);

#[non_exhaustive]
#[derive(Debug)]
pub enum Value {
	Null,
	Bool(bool),
	Number(Number),
	Object(Object),
	Array(Array),
	Uuid(Uuid),
	Datetime(Datetime),
	Duration(Duration),
	Bytes(Bytes),
	RecordId(RecordId),
}
