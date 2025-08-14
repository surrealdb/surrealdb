use std::vec::IntoIter;

use anyhow::{Result, bail};

use crate::err::Error;
use crate::val::Value;
use crate::val::value::{Cast as CastTrait, Coerce};

/// The number of arguments a function takes.
#[derive(Debug)]
pub struct Arity {
	pub lower: usize,
	pub upper: Option<usize>,
}

impl Arity {
	pub const fn base() -> Arity {
		Arity {
			lower: 0,
			upper: Some(0),
		}
	}

	/// Combine the arity from multiple arugments to calculate the combined
	/// arity.
	pub fn combine(self, other: Self) -> Arity {
		Arity {
			lower: self.lower + other.lower,
			upper: self.upper.and_then(|a| other.upper.map(|b| a + b)),
		}
	}
}

pub struct Args {
	next: Option<Value>,
	count: usize,
	iter: IntoIter<Value>,
}

impl Args {
	pub fn from_vec(args: Vec<Value>) -> Self {
		Args {
			next: None,
			count: 1,
			iter: args.into_iter(),
		}
	}

	pub fn has_next(&mut self) -> bool {
		self.next.is_some() || {
			self.next = self.iter.next();
			self.next.is_some()
		}
	}

	pub fn peek(&mut self) -> Option<&Value> {
		if self.next.is_none() {
			self.next = self.iter.next();
		}
		self.next.as_ref()
	}

	pub fn next(&mut self) -> Option<(usize, Value)> {
		let v = self.next.take().or_else(|| self.iter.next())?;
		let idx = self.count;
		self.count += 1;
		Some((idx, v))
	}
}

pub trait FromArg: Sized {
	// returns the number of arguments the type takes.
	fn arity() -> Arity;
	/// Convert a collection of argument values into a certain argument format,
	/// failing if there are too many or too few arguments, or if one of the
	/// arguments could not be converted.
	fn from_arg(name: &str, args: &mut Args) -> Result<Self>;
}

pub trait FromArgs: Sized {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self>;
}

/// A wrapper type for optional arguments, as opposed to Option which might also
/// indicate None being a proper value.
#[repr(transparent)]
pub struct Optional<T>(pub Option<T>);

impl<T: FromArg> FromArg for Optional<T> {
	fn arity() -> Arity {
		let mut res = T::arity();
		res.lower = 0;
		res
	}

	fn from_arg(name: &str, arg: &mut Args) -> Result<Self> {
		if !arg.has_next() {
			return Ok(Optional(None));
		}

		if T::arity().lower == 1 {
			if let Some(Value::None) = arg.peek() {
				return Ok(Optional(None));
			}
		}

		let v = T::from_arg(name, arg)?;
		Ok(Optional(Some(v)))
	}
}

/// A wrapper type for remaining arguments, will collect all arguments which
/// remain.
#[repr(transparent)]
pub struct Rest<T>(pub Vec<T>);

impl<T: Coerce> FromArg for Rest<T> {
	fn arity() -> Arity {
		Arity {
			lower: 0,
			upper: None,
		}
	}

	fn from_arg(name: &str, iter: &mut Args) -> Result<Self> {
		let mut res = Vec::new();
		while let Some((idx, x)) = iter.next() {
			let v = x.coerce_to::<T>().map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument {idx} was the wrong type. {e}"),
			})?;
			res.push(v);
		}
		Ok(Rest(res))
	}
}

impl<T: Coerce> FromArg for T {
	fn arity() -> Arity {
		Arity {
			lower: 1,
			upper: Some(1),
		}
	}

	fn from_arg(name: &str, iter: &mut Args) -> Result<Self> {
		// The error should not happen when called with the FromArgs traits as the arity
		// is already checked.
		let (idx, x) = iter.next().ok_or_else(|| Error::InvalidArguments {
			name: name.to_owned(),
			message: "Missing an argument".to_string(),
		})?;

		let v = x.coerce_to::<T>().map_err(|e| Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Argument {idx} was the wrong type. {e}"),
		})?;
		Ok(v)
	}
}

/// Wrapper type for arguments which use coercing rules instead of coercing
/// rules.
pub struct Cast<T>(pub T);

impl<T: CastTrait> FromArg for Cast<T> {
	fn arity() -> Arity {
		Arity {
			lower: 1,
			upper: Some(1),
		}
	}

	fn from_arg(name: &str, iter: &mut Args) -> Result<Self> {
		// The error should not happen when called with the FromArgs traits as the arity
		// is already checked.
		let (idx, x) = iter.next().ok_or_else(|| Error::InvalidArguments {
			name: name.to_owned(),
			message: "Missing an argument".to_string(),
		})?;

		let v = x.cast_to::<T>().map_err(|e| Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Argument {idx} was the wrong type. {e}"),
		})?;
		Ok(Cast(v))
	}
}

impl<T: FromArg> FromArgs for T {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self> {
		let arity = T::arity();

		if args.len() < arity.lower || arity.upper.map(|x| args.len() > x).unwrap_or(false) {
			let message = if let Some(upper) = arity.upper {
				if upper == arity.lower {
					if upper == 0 {
						"Expected no arguments".to_string()
					} else if upper == 1 {
						"Expected 1 argument".to_string()
					} else {
						format!("Expected {upper} arguments")
					}
				} else {
					format!("Expected {} to {} arguments", arity.lower, upper)
				}
			} else if arity.lower == 0 {
				"Expected zero or more arguments".to_string()
			} else {
				format!("Expected {} or more arguments", arity.lower)
			};

			bail!(Error::InvalidArguments {
				name: name.to_owned(),
				message,
			});
		}

		let mut args = Args::from_vec(args);
		T::from_arg(name, &mut args)
	}
}

/// A wrapper type for functions which do their own typechecking of arguments.
/// Take ownership of the raw arguments collection, and assume responsibility of
/// validating the number of arguments and converting them as necessary.
#[repr(transparent)]
pub struct Any(pub Vec<Value>);

// Take ownership of the raw arguments collection, and assume responsibility of
// validating the number of arguments and converting them as necessary.
impl FromArgs for Any {
	fn from_args(_name: &str, args: Vec<Value>) -> Result<Self> {
		Ok(Any(args))
	}
}

/// Some functions take a fixed number of arguments.
/// The len must match the number of type idents that follow.
macro_rules! impl_tuple {
	($($T:ident), *$(,)?) => {

		impl<$($T:FromArg),*> FromArg for ($($T,)*) {
			fn arity() -> Arity{
				Arity::base()
				$(
					.combine($T::arity())
				)*
			}

			#[allow(non_snake_case)]
			fn from_arg(_name: &str, _iter: &mut Args) -> Result<Self>
			{
				Ok(( $(
					$T::from_arg(_name,_iter)?,
				)*))
			}
		}
	}
}

// It is possible to add larger sequences to support higher quantities of fixed
// arguments.
impl_tuple!();
impl_tuple!(A);
impl_tuple!(A, B);
impl_tuple!(A, B, C);
impl_tuple!(A, B, C, D);
