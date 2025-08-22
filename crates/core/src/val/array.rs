use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Deref, DerefMut};

use anyhow::{Result, ensure};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::expr::Expr;
use crate::expr::fmt::{Fmt, Pretty, pretty_indent};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Array")]
pub struct Array(pub Vec<Value>);

impl From<Value> for Array {
	fn from(v: Value) -> Self {
		vec![v].into()
	}
}

impl<T> From<Vec<T>> for Array
where
	Value: From<T>,
{
	fn from(v: Vec<T>) -> Self {
		v.into_iter().map(Value::from).collect()
	}
}

impl From<Array> for Vec<Value> {
	fn from(s: Array) -> Self {
		s.0
	}
}

impl FromIterator<Value> for Array {
	fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
		Array(iter.into_iter().collect())
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
	// Create a new empty array
	pub fn new() -> Self {
		Self::default()
	}
	// Create a new array with capacity
	pub fn with_capacity(len: usize) -> Self {
		Self(Vec::with_capacity(len))
	}
	// Get the length of the array
	pub fn len(&self) -> usize {
		self.0.len()
	}
	// Check if there array is empty
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn into_literal(self) -> Vec<Expr> {
		self.into_iter().map(|x| x.into_literal()).collect()
	}

	pub(crate) fn is_all_none_or_null(&self) -> bool {
		self.0.iter().all(|v| v.is_nullish())
	}

	/// Removes all values in the array which are equal to the given value.
	pub fn remove_value(mut self, other: &Value) -> Self {
		self.retain(|x| x != other);
		self
	}

	/// Removes all values in the array which are equal to a value in the given slice.
	pub fn remove_all(mut self, other: &[Value]) -> Self {
		self.retain(|x| !other.contains(x));
		self
	}

	/// Concatenates the two arrays returning an array with the values of both arrays.
	pub fn concat(mut self, mut other: Array) -> Self {
		self.0.append(&mut other.0);
		self
	}

	/// Pushes a value but takes self as a value.
	pub fn with_push(mut self, other: Value) -> Self {
		self.0.push(other);
		self
	}

	pub(crate) fn display<V: Display>(v: &[V], f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		f.write_char('[')?;
		if !v.is_empty() {
			let indent = pretty_indent();
			write!(f, "{}", Fmt::pretty_comma_separated(v))?;
			drop(indent);
		}
		f.write_char(']')
	}
}

impl Display for Array {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Array::display(&self.0, f)
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

pub(crate) trait Clump<T> {
	fn clump(self, clump_size: usize) -> Result<T>;
}

impl Clump<Array> for Array {
	fn clump(self, clump_size: usize) -> Result<Array> {
		ensure!(
			clump_size >= 1,
			Error::InvalidArguments {
				name: "array::clump".to_string(),
				message: "The second argument must be an integer greater than 0".to_string(),
			}
		);

		Ok(self
			.0
			.chunks(clump_size)
			.map::<Value, _>(|chunk| chunk.to_vec().into())
			.collect::<Vec<_>>()
			.into())
	}
}

// ------------------------------

pub(crate) trait Combine<T> {
	fn combine(self, other: T) -> T;
}

impl Combine<Array> for Array {
	fn combine(self, other: Self) -> Array {
		let mut out = Self::with_capacity(self.len().saturating_mul(other.len()));
		for a in self.iter() {
			for b in other.iter() {
				out.push(vec![a.clone(), b.clone()].into());
			}
		}
		out
	}
}

// ------------------------------

pub(crate) trait Complement<T> {
	fn complement(self, other: T) -> T;
}

impl Complement<Array> for Array {
	#[expect(clippy::mutable_key_type)]
	fn complement(self, other: Self) -> Array {
		let mut out = Array::with_capacity(self.len());
		let mut set = BTreeSet::new();
		for i in other.iter() {
			set.insert(i);
		}
		for v in self.into_iter() {
			if !set.contains(&v) {
				out.push(v)
			}
		}
		out
	}
}

// ------------------------------

pub(crate) trait Difference<T> {
	fn difference(self, other: T) -> T;
}

impl Difference<Array> for Array {
	fn difference(self, other: Array) -> Array {
		let mut out = Array::with_capacity(self.len() + other.len());
		let mut other = VecDeque::from(other.0);
		for v in self.into_iter() {
			if let Some(pos) = other.iter().position(|w| v == *w) {
				other.remove(pos);
			} else {
				out.push(v);
			}
		}
		out.append(&mut Vec::from(other));
		out
	}
}

// ------------------------------

pub(crate) trait Flatten<T> {
	fn flatten(self) -> T;
}

impl Flatten<Array> for Array {
	fn flatten(self) -> Array {
		let mut out = Array::with_capacity(self.len());
		for v in self.into_iter() {
			match v {
				Value::Array(mut a) => out.append(&mut a),
				_ => out.push(v),
			}
		}
		out
	}
}

// ------------------------------

pub(crate) trait Intersect<T> {
	fn intersect(self, other: T) -> T;
}

impl Intersect<Self> for Array {
	fn intersect(self, mut other: Self) -> Self {
		let mut out = Self::new();
		for v in self.0.into_iter() {
			if let Some(pos) = other.iter().position(|w| v == *w) {
				other.remove(pos);
				out.push(v);
			}
		}
		out
	}
}

// ------------------------------

// Documented with the assumption that it is just for arrays.
pub(crate) trait Matches<T> {
	/// Returns an array complimenting the original where each value is true or
	/// false depending on whether it is == to the compared value.
	///
	/// Admittedly, this is most often going to be used in
	/// `count(array::matches($arr, $val))` to count the number of times an
	/// element appears in an array but it's nice to have this in addition.
	fn matches(self, compare_val: Value) -> T;
}

impl Matches<Array> for Array {
	fn matches(self, compare_val: Value) -> Array {
		self.iter().map(|arr_val| (arr_val == &compare_val).into()).collect::<Vec<Value>>().into()
	}
}

// ------------------------------

// Documented with the assumption that it is just for arrays.
pub(crate) trait Transpose<T> {
	/// Stacks arrays on top of each other. This can serve as 2d array
	/// transposition.
	///
	/// The input array can contain regular values which are treated as arrays
	/// with a single element.
	///
	/// It's best to think of the function as creating a layered structure of
	/// the arrays rather than transposing them when the input is not a 2d
	/// array. See the examples for what happense when the input arrays are not
	/// all the same size.
	///
	/// Here's a diagram:
	/// [0, 1, 2, 3], [4, 5, 6]
	/// ->
	/// [0    | 1    | 2   |  3]
	/// [4    | 5    | 6   ]
	///  ^      ^      ^      ^
	/// [0, 4] [1, 5] [2, 6] [3]
	///
	/// # Examples
	///
	/// ```ignore
	/// fn array(sql: &str) -> Array {
	///     unimplemented!();
	/// }
	///
	/// // Example of `transpose` doing what it says on the tin.
	/// assert_eq!(array("[[0, 1], [2, 3]]").transpose(), array("[[0, 2], [1, 3]]"));
	/// // `transpose` can be thought of layering arrays on top of each other so when
	/// // one array runs out, it stops appearing in the output.
	/// assert_eq!(array("[[0, 1], [2]]").transpose(), array("[[0, 2], [1]]"));
	/// assert_eq!(array("[0, 1, 2]").transpose(), array("[[0, 1, 2]]"));
	/// ```
	fn transpose(self) -> T;
}

impl Transpose<Array> for Array {
	fn transpose(self) -> Array {
		if self.is_empty() {
			return self;
		}
		// I'm sure there's a way more efficient way to do this that I don't know about.
		// The new array will be at *least* this large so we can start there;
		let mut transposed_vec = Vec::<Value>::with_capacity(self.len());
		let mut iters = self
			.iter()
			.map(|v| {
				if let Value::Array(arr) = v {
					Box::new(arr.iter().cloned()) as Box<dyn ExactSizeIterator<Item = Value>>
				} else {
					Box::new(std::iter::once(v).cloned())
						as Box<dyn ExactSizeIterator<Item = Value>>
				}
			})
			.collect::<Vec<_>>();
		// We know there is at least one element in the array therefore iters is not
		// empty. This is safe.
		let longest_length = iters.iter().map(|i| i.len()).max().unwrap();
		for _ in 0..longest_length {
			transposed_vec.push(
				iters
					.iter_mut()
					.map(|i| i.next().unwrap_or(Value::None))
					.collect::<Vec<_>>()
					.into(),
			);
		}
		transposed_vec.into()
	}
}

// ------------------------------

pub(crate) trait Union<T> {
	fn union(self, other: T) -> T;
}

impl Union<Self> for Array {
	fn union(mut self, mut other: Self) -> Array {
		self.append(&mut other);
		self.uniq()
	}
}

// ------------------------------

pub(crate) trait Uniq<T> {
	fn uniq(self) -> T;
}

impl Uniq<Array> for Array {
	fn uniq(self) -> Array {
		#[expect(clippy::mutable_key_type)]
		let mut set = HashSet::with_capacity(self.len());
		let mut to_return = Array::with_capacity(self.len());
		for i in self.iter() {
			if set.insert(i) {
				to_return.push(i.clone());
			}
		}
		to_return
	}
}

// ------------------------------

pub(crate) trait Windows<T> {
	fn windows(self, window_size: usize) -> Result<T>;
}

impl Windows<Array> for Array {
	fn windows(self, window_size: usize) -> Result<Array> {
		ensure!(
			window_size >= 1,
			Error::InvalidArguments {
				name: "array::windows".to_string(),
				message: "The second argument must be an integer greater than 0".to_string(),
			}
		);

		Ok(self
			.0
			.windows(window_size)
			.map::<Value, _>(|chunk| chunk.to_vec().into())
			.collect::<Vec<_>>()
			.into())
	}
}
