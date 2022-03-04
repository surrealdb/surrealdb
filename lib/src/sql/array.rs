use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::number::Number;
use crate::sql::operation::Operation;
use crate::sql::strand::Strand;
use crate::sql::value::{value, Value};
use nom::bytes::complete::tag;
use nom::combinator::opt;
use nom::multi::separated_list0;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Array {
	pub value: Vec<Value>,
}

impl From<Vec<Value>> for Array {
	fn from(v: Vec<Value>) -> Self {
		Array {
			value: v,
		}
	}
}

impl From<Vec<i32>> for Array {
	fn from(v: Vec<i32>) -> Self {
		Array {
			value: v.into_iter().map(|x| x.into()).collect(),
		}
	}
}

impl From<Vec<String>> for Array {
	fn from(v: Vec<String>) -> Self {
		Array {
			value: v.into_iter().map(|x| x.into()).collect(),
		}
	}
}

impl From<Vec<Vec<Value>>> for Array {
	fn from(v: Vec<Vec<Value>>) -> Self {
		Array {
			value: v.into_iter().map(|x| x.into()).collect(),
		}
	}
}

impl<'a> From<Vec<&str>> for Array {
	fn from(v: Vec<&str>) -> Self {
		Array {
			value: v.into_iter().map(Value::from).collect(),
		}
	}
}

impl From<Vec<Operation>> for Array {
	fn from(v: Vec<Operation>) -> Self {
		Array {
			value: v.into_iter().map(Value::from).collect(),
		}
	}
}

impl Array {
	pub fn len(&self) -> usize {
		self.value.len()
	}

	pub fn is_empty(&self) -> bool {
		self.value.is_empty()
	}

	pub fn as_ints(self) -> Vec<i64> {
		self.value.into_iter().map(|v| v.as_int()).collect()
	}

	pub fn as_floats(self) -> Vec<f64> {
		self.value.into_iter().map(|v| v.as_float()).collect()
	}

	pub fn as_numbers(self) -> Vec<Number> {
		self.value.into_iter().map(|v| v.as_number()).collect()
	}

	pub fn as_strands(self) -> Vec<Strand> {
		self.value.into_iter().map(|v| v.as_strand()).collect()
	}

	pub fn as_point(mut self) -> [f64; 2] {
		match self.len() {
			0 => [0.0, 0.0],
			1 => [self.value.remove(0).as_float(), 0.0],
			_ => [self.value.remove(0).as_float(), self.value.remove(0).as_float()],
		}
	}

	pub fn to_operations(&self) -> Result<Vec<Operation>, Error> {
		self.value
			.iter()
			.map(|v| match v {
				Value::Object(v) => v.to_operation(),
				_ => Err(Error::PatchError {
					message: String::from("Operation must be an object"),
				}),
			})
			.collect::<Result<Vec<_>, Error>>()
	}
}

impl Array {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		let mut x = Vec::new();
		for v in &self.value {
			match v.compute(ctx, opt, txn, doc).await {
				Ok(v) => x.push(v),
				Err(e) => return Err(e),
			};
		}
		Ok(Value::Array(Array {
			value: x,
		}))
	}
}

impl fmt::Display for Array {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"[{}]",
			self.value.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
		)
	}
}

impl Serialize for Array {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.value)
		} else {
			let mut val = serializer.serialize_struct("Array", 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

// ------------------------------

impl ops::Add<Value> for Array {
	type Output = Self;
	fn add(mut self, other: Value) -> Self {
		if !self.value.iter().any(|x| *x == other) {
			self.value.push(other)
		}
		self
	}
}

impl ops::Add for Array {
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		for v in other.value {
			if !self.value.iter().any(|x| *x == v) {
				self.value.push(v)
			}
		}
		self
	}
}

// ------------------------------

impl ops::Sub<Value> for Array {
	type Output = Self;
	fn sub(mut self, other: Value) -> Self {
		if let Some(p) = self.value.iter().position(|x| *x == other) {
			self.value.remove(p);
		}
		self
	}
}

impl ops::Sub for Array {
	type Output = Self;
	fn sub(mut self, other: Self) -> Self {
		for v in other.value {
			if let Some(p) = self.value.iter().position(|x| *x == v) {
				self.value.remove(p);
			}
		}
		self
	}
}

// ------------------------------

pub trait Abolish<T> {
	fn abolish<F>(&mut self, f: F)
	where
		F: FnMut(usize) -> bool;
}

impl<T> Abolish<T> for Vec<T> {
	fn abolish<F>(&mut self, mut f: F)
	where
		F: FnMut(usize) -> bool,
	{
		let len = self.len();
		let mut del = 0;
		{
			let v = &mut **self;

			for i in 0..len {
				if f(i) {
					del += 1;
				} else if del > 0 {
					v.swap(i - del, i);
				}
			}
		}
		if del > 0 {
			self.truncate(len - del);
		}
	}
}

// ------------------------------

pub trait Uniq<T> {
	fn uniq(self) -> Vec<T>;
}

impl<T: PartialEq> Uniq<T> for Vec<T> {
	fn uniq(mut self) -> Vec<T> {
		for x in (0..self.len()).rev() {
			for y in (x + 1..self.len()).rev() {
				if self[x] == self[y] {
					self.remove(y);
				}
			}
		}
		self
	}
}

// ------------------------------

pub trait Union<T> {
	fn union(self, other: Vec<T>) -> Vec<T>;
}

impl<T: PartialEq> Union<T> for Vec<T> {
	fn union(mut self, mut other: Vec<T>) -> Vec<T> {
		self.append(&mut other);
		self.uniq()
	}
}

// ------------------------------

pub trait Combine<T> {
	fn combine(self, other: Vec<T>) -> Vec<Vec<T>>;
}

impl<T: PartialEq + Clone> Combine<T> for Vec<T> {
	fn combine(self, other: Vec<T>) -> Vec<Vec<T>> {
		let mut out = Vec::new();
		for a in self.iter() {
			for b in other.iter() {
				if a != b {
					out.push(vec![a.clone(), b.clone()]);
				}
			}
		}
		out
	}
}

// ------------------------------

pub trait Concat<T> {
	fn concat(self, other: Vec<T>) -> Vec<Vec<T>>;
}

impl<T: PartialEq + Clone> Concat<T> for Vec<T> {
	fn concat(self, other: Vec<T>) -> Vec<Vec<T>> {
		let mut out = Vec::new();
		for a in self.iter() {
			for b in other.iter() {
				out.push(vec![a.clone(), b.clone()]);
			}
		}
		out
	}
}

// ------------------------------

pub trait Intersect<T> {
	fn intersect(self, other: Vec<T>) -> Vec<T>;
}

impl<T: PartialEq> Intersect<T> for Vec<T> {
	fn intersect(self, other: Vec<T>) -> Vec<T> {
		let mut out = Vec::new();
		let mut other: Vec<_> = other.into_iter().collect();
		for a in self.into_iter() {
			if let Some(pos) = other.iter().position(|b| a == *b) {
				out.push(a);
				other.remove(pos);
			}
		}
		out
	}
}

// ------------------------------

pub trait Difference<T> {
	fn difference(self, other: Vec<T>) -> Vec<T>;
}

impl<T: PartialEq> Difference<T> for Vec<T> {
	fn difference(self, other: Vec<T>) -> Vec<T> {
		let mut out = Vec::new();
		let mut other: Vec<_> = other.into_iter().collect();
		for a in self.into_iter() {
			if let Some(pos) = other.iter().position(|b| a == *b) {
				other.remove(pos);
			} else {
				out.push(a);
			}
		}
		out.append(&mut other);
		out
	}
}

// ------------------------------

pub fn array(i: &str) -> IResult<&str, Array> {
	let (i, _) = tag("[")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(tag(","))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((
		i,
		Array {
			value: v,
		},
	))
}

fn item(i: &str) -> IResult<&str, Value> {
	let (i, v) = value(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn array_normal() {
		let sql = "[1,2,3]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn array_commas() {
		let sql = "[1,2,3,]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn array_expression() {
		let sql = "[1,2,3+1]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3 + 1]", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}
}
