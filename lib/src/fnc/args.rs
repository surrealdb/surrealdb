use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::{Array, Datetime, Duration, Number, Strand, Thing};

/// Implemented by types that are commonly used, in a certain way, as arguments.
pub trait FromArg: Sized {
	fn from_arg(arg: Value) -> Result<Self, Error>;
}

impl FromArg for Value {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg)
	}
}

impl FromArg for String {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_string()
	}
}

impl FromArg for Strand {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_strand()
	}
}

impl FromArg for Number {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_number()
	}
}

impl FromArg for Datetime {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_datetime()
	}
}

impl FromArg for Duration {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_duration()
	}
}

impl FromArg for Thing {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_record()
	}
}

impl FromArg for Array {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_array()
	}
}

impl FromArg for i64 {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_i64()
	}
}

impl FromArg for u64 {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_u64()
	}
}

impl FromArg for f64 {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_f64()
	}
}

impl FromArg for isize {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.convert_to_i64()? as isize)
	}
}

impl FromArg for usize {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.convert_to_u64()? as usize)
	}
}

impl FromArg for Vec<Number> {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		arg.convert_to_array()?.into_iter().map(Value::convert_to_number).collect()
	}
}

pub trait FromArgs: Sized {
	/// Convert a collection of argument values into a certain argument format, failing if there are
	/// too many or too few arguments, or if one of the arguments could not be converted.
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error>;
}

// Take ownership of the raw arguments collection, and assume responsibility of validating the
// number of arguments and converting them as necessary.
impl FromArgs for Vec<Value> {
	fn from_args(_name: &str, args: Vec<Value>) -> Result<Self, Error> {
		Ok(args)
	}
}

/// Some functions take a fixed number of arguments.
/// The len must match the number of type idents that follow.
macro_rules! impl_tuple {
	($len:expr, $( $T:ident ),*) => {
		impl<$($T:FromArg),*> FromArgs for ($($T,)*) {
			#[allow(non_snake_case)]
			fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
				let [$($T),*]: [Value; $len] = args.try_into().map_err(|_| Error::InvalidArguments {
					name: name.to_owned(),
					// This match will be optimized away.
					message: match $len {
						0 => String::from("Expected no arguments."),
						1 => String::from("Expected 1 argument."),
						_ => format!("Expected {} arguments.", $len),
					}
				})?;
				#[allow(unused_mut, unused_variables)]
				let mut i = 0;
				Ok((
					$({
						i += 1;
						$T::from_arg($T).map_err(|e| Error::InvalidArguments {
							name: name.to_owned(),
							message: format!("Argument {i} was the wrong type. {e}"),
						})?
					},)*
				))
			}
		}
	}
}

// It is possible to add larger sequences to support higher quantities of fixed arguments.
impl_tuple!(0,);
impl_tuple!(1, A);
impl_tuple!(2, A, B);
impl_tuple!(3, A, B, C);

// Some functions take a single, optional argument, or no arguments at all.
impl<A: FromArg> FromArgs for (Option<A>,) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 0 or 1 arguments."),
		};
		// Process the function arguments
		let mut args = args.into_iter();
		// Process the first function argument
		let a = match args.next() {
			Some(a) => Some(A::from_arg(a).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process additional function arguments
		if args.next().is_some() {
			// Too many arguments
			return Err(err());
		}
		Ok((a,))
	}
}

// Some functions take 1 or 2 arguments, so the second argument is optional.
impl<A: FromArg, B: FromArg> FromArgs for (A, Option<B>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 1 or 2 arguments."),
		};
		// Process the function arguments
		let mut args = args.into_iter();
		// Process the first argument
		let a = A::from_arg(args.next().ok_or_else(err)?).map_err(|e| Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b)?),
			None => None,
		};
		// Process additional function arguments
		if args.next().is_some() {
			// Too many arguments
			return Err(err());
		}
		Ok((a, b))
	}
}

// Some functions take 2 or 3 arguments, so the third argument is optional.
impl<A: FromArg, B: FromArg, C: FromArg> FromArgs for (A, B, Option<C>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 2 or 3 arguments."),
		};
		// Process the function arguments
		let mut args = args.into_iter();
		// Process the first function argument
		let a = A::from_arg(args.next().ok_or_else(err)?).map_err(|e| Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;
		// Process the second function argument
		let b = B::from_arg(args.next().ok_or_else(err)?).map_err(|e| Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Argument 2 was the wrong type. {e}"),
		})?;
		// Process the third function argument
		let c = match args.next() {
			Some(c) => Some(C::from_arg(c).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 3 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process additional function arguments
		if args.next().is_some() {
			// Too many arguments
			return Err(err());
		}
		Ok((a, b, c))
	}
}

// Some functions take 0, 1, or 2 arguments, so both arguments are optional.
// It is safe to assume that, if the first argument is None, the second argument will also be None.
impl<A: FromArg, B: FromArg> FromArgs for (Option<A>, Option<B>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 0, 1, or 2 arguments."),
		};
		// Process the function arguments
		let mut args = args.into_iter();
		// Process the first function argument
		let a = match args.next() {
			Some(a) => Some(A::from_arg(a).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process the second function argument
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 2 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process additional function arguments
		if args.next().is_some() {
			// Too many arguments
			return Err(err());
		}
		Ok((a, b))
	}
}

// Some functions optionally take 2 arguments, or don't take any at all.
impl<A: FromArg, B: FromArg> FromArgs for (Option<(A, B)>,) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 0 or 2 arguments."),
		};
		// Process the function arguments
		let mut args = args.into_iter();
		// Process the first function argument
		let a = match args.next() {
			Some(a) => Some(A::from_arg(a).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process the second function argument
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 2 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process additional function arguments
		if a.is_some() != b.is_some() || args.next().is_some() {
			// One argument, or too many arguments
			return Err(err());
		}
		Ok((a.zip(b),))
	}
}

// Some functions take 1, 2, or 3 arguments. It is safe to assume that, if the second argument is
// None, the third argument will also be None.
impl<A: FromArg, B: FromArg, C: FromArg> FromArgs for (A, Option<B>, Option<C>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 1, 2, or 3 arguments."),
		};
		// Process the function arguments
		let mut args = args.into_iter();
		// Process the first function argument
		let a = A::from_arg(args.next().ok_or_else(err)?).map_err(|e| Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;
		// Process the second function argument
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 2 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process the third function argument
		let c = match args.next() {
			Some(c) => Some(C::from_arg(c).map_err(|e| Error::InvalidArguments {
				name: name.to_owned(),
				message: format!("Argument 3 was the wrong type. {e}"),
			})?),
			None => None,
		};
		// Process additional function arguments
		if args.next().is_some() {
			// Too many arguments
			return Err(err());
		}
		Ok((a, b, c))
	}
}
