use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::number::Number;
use crate::sql::operation::Operation;
use crate::sql::serde::is_internal_serialization;
use crate::sql::strand::Strand;
use crate::sql::value::{value, Value};
use nom::character::complete::char;
use nom::combinator::opt;
use nom::multi::separated_list0;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;
use std::ops::Deref;
use std::ops::DerefMut;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct Array(pub Vec<Value>);

impl From<Value> for Array {
	fn from(v: Value) -> Self {
		Array(vec![v])
	}
}

impl From<Vec<Value>> for Array {
	fn from(v: Vec<Value>) -> Self {
		Array(v)
	}
}

impl From<Vec<i32>> for Array {
	fn from(v: Vec<i32>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl From<Vec<&str>> for Array {
	fn from(v: Vec<&str>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl From<Vec<Number>> for Array {
	fn from(v: Vec<Number>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl From<Vec<Operation>> for Array {
	fn from(v: Vec<Operation>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl Deref for Array {
	type Target = Vec<Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Array {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl IntoIterator for Array {
	type Item = Value;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Array {
	pub fn new() -> Self {
		Array(Vec::default())
	}

	pub fn with_capacity(len: usize) -> Self {
		Array(Vec::with_capacity(len))
	}

	pub fn as_ints(self) -> Vec<i64> {
		self.0.into_iter().map(|v| v.as_int()).collect()
	}

	pub fn as_floats(self) -> Vec<f64> {
		self.0.into_iter().map(|v| v.as_float()).collect()
	}

	pub fn as_numbers(self) -> Vec<Number> {
		self.0.into_iter().map(|v| v.as_number()).collect()
	}

	pub fn as_strands(self) -> Vec<Strand> {
		self.0.into_iter().map(|v| v.as_strand()).collect()
	}

	pub fn as_point(mut self) -> [f64; 2] {
		match self.len() {
			0 => [0.0, 0.0],
			1 => [self.0.remove(0).as_float(), 0.0],
			_ => [self.0.remove(0).as_float(), self.0.remove(0).as_float()],
		}
	}
}

impl Array {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		let mut x = Vec::new();
		for v in self.iter() {
			match v.compute(ctx, opt, txn, doc).await {
				Ok(v) => x.push(v),
				Err(e) => return Err(e),
			};
		}
		Ok(Value::Array(Array(x)))
	}
}

impl fmt::Display for Array {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[{}]", self.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

impl Serialize for Array {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct("Array", &self.0)
		} else {
			serializer.serialize_some(&self.0)
		}
	}
}

// ------------------------------

impl ops::Add<Value> for Array {
	type Output = Self;
	fn add(mut self, other: Value) -> Self {
		if !self.0.iter().any(|x| *x == other) {
			self.0.push(other)
		}
		self
	}
}

impl ops::Add for Array {
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		for v in other.0 {
			if !self.0.iter().any(|x| *x == v) {
				self.0.push(v)
			}
		}
		self
	}
}

// ------------------------------

impl ops::Sub<Value> for Array {
	type Output = Self;
	fn sub(mut self, other: Value) -> Self {
		if let Some(p) = self.0.iter().position(|x| *x == other) {
			self.0.remove(p);
		}
		self
	}
}

impl ops::Sub for Array {
	type Output = Self;
	fn sub(mut self, other: Self) -> Self {
		for v in other.0 {
			if let Some(p) = self.0.iter().position(|x| *x == v) {
				self.0.remove(p);
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
		let mut i = 0;
		// FIXME: use drain_filter once stabilized (https://github.com/rust-lang/rust/issues/43244)
		// to avoid negation of the predicate return value.
		self.retain(|_| {
			let retain = !f(i);
			i += 1;
			retain
		});
	}
}

// ------------------------------

pub trait Combine<T> {
	fn combine(self, other: T) -> T;
}

impl Combine<Array> for Array {
	fn combine(self, other: Array) -> Array {
		let mut out = Array::new();
		for a in self.iter() {
			for b in other.iter() {
				out.push(vec![a.clone(), b.clone()].into());
			}
		}
		out
	}
}

// ------------------------------

pub trait Concat<T> {
	fn concat(self, other: T) -> T;
}

impl Concat<Array> for Array {
	fn concat(mut self, mut other: Array) -> Array {
		self.append(&mut other);
		self
	}
}

// ------------------------------

pub trait Difference<T> {
	fn difference(self, other: T) -> T;
}

impl Difference<Array> for Array {
	fn difference(self, other: Array) -> Array {
		let mut out = Array::new();
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

pub trait Intersect<T> {
	fn intersect(self, other: T) -> T;
}

impl Intersect<Array> for Array {
	fn intersect(self, other: Array) -> Array {
		let mut out = Array::new();
		let mut other: Vec<_> = other.into_iter().collect();
		for a in self.0.into_iter() {
			if let Some(pos) = other.iter().position(|b| a == *b) {
				out.push(a);
				other.remove(pos);
			}
		}
		out
	}
}

// ------------------------------

pub trait Union<T> {
	fn union(self, other: T) -> T;
}

impl Union<Array> for Array {
	fn union(mut self, mut other: Array) -> Array {
		self.append(&mut other);
		self.uniq()
	}
}

// ------------------------------

pub trait Uniq<T> {
	fn uniq(self) -> T;
}

impl Uniq<Array> for Array {
	fn uniq(mut self) -> Array {
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

pub fn array(i: &str) -> IResult<&str, Array> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, Array(v)))
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
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_commas() {
		let sql = "[1,2,3,]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_expression() {
		let sql = "[1,2,3+1]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3 + 1]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}
}
