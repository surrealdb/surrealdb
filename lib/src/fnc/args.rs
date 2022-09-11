use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::{Number, Strand};

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
		Ok(arg.as_string())
	}
}

impl FromArg for Strand {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.as_strand())
	}
}

impl FromArg for Number {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.as_number())
	}
}

impl FromArg for f64 {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.as_float())
	}
}

impl FromArg for i64 {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.as_int())
	}
}

impl FromArg for usize {
	fn from_arg(arg: Value) -> Result<Self, Error> {
		Ok(arg.as_int() as usize)
	}
}

pub fn shim<A: FromArgs>(name: &str, args: Vec<Value>) -> Result<A, Error> {
	A::from_args(name, args)
}

pub trait FromArgs: Sized {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error>;
}

impl FromArgs for Vec<Value> {
	fn from_args(_name: &str, args: Vec<Value>) -> Result<Self, Error> {
		Ok(args)
	}
}

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
				Ok(($($T::from_arg($T)?,)*))
			}
		}
	}
}

impl_tuple!(0,);
impl_tuple!(1, A);
impl_tuple!(2, A, B);
impl_tuple!(3, A, B, C);

impl<A: FromArg> FromArgs for (Option<A>,) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 0 or 1 arguments."),
		};

		let mut args = args.into_iter();
		let a = match args.next() {
			Some(a) => Some(A::from_arg(a)?),
			None => None,
		};
		if args.next().is_some() {
			// Too many.
			return Err(err());
		}
		Ok((a,))
	}
}

impl<A: FromArg, B: FromArg> FromArgs for (A, Option<B>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 1 or 2 arguments."),
		};

		let mut args = args.into_iter();
		let a = A::from_arg(args.next().ok_or_else(err)?)?;
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b)?),
			None => None,
		};
		if args.next().is_some() {
			// Too many.
			return Err(err());
		}
		Ok((a, b))
	}
}

impl<A: FromArg, B: FromArg> FromArgs for (Option<A>, Option<B>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 0, 1, or 2 arguments."),
		};

		let mut args = args.into_iter();
		let a = match args.next() {
			Some(a) => Some(A::from_arg(a)?),
			None => None,
		};
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b)?),
			None => None,
		};
		if args.next().is_some() {
			// Too many.
			return Err(err());
		}
		Ok((a, b))
	}
}

impl<A: FromArg, B: FromArg> FromArgs for (Option<(A, B)>,) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 0 or 2 arguments."),
		};

		let mut args = args.into_iter();
		let a = match args.next() {
			Some(a) => Some(A::from_arg(a)?),
			None => None,
		};
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b)?),
			None => None,
		};
		if a.is_some() != b.is_some() || args.next().is_some() {
			// One argument, or too many arguments.
			return Err(err());
		}
		Ok((a.zip(b),))
	}
}

impl<A: FromArg, B: FromArg, C: FromArg> FromArgs for (A, Option<B>, Option<C>) {
	fn from_args(name: &str, args: Vec<Value>) -> Result<Self, Error> {
		let err = || Error::InvalidArguments {
			name: name.to_owned(),
			message: String::from("Expected 1, 2, or 3 arguments."),
		};

		let mut args = args.into_iter();
		let a = A::from_arg(args.next().ok_or_else(err)?)?;
		let b = match args.next() {
			Some(b) => Some(B::from_arg(b)?),
			None => None,
		};
		let c = match args.next() {
			Some(c) => Some(C::from_arg(c)?),
			None => None,
		};
		if args.next().is_some() {
			// Too many.
			return Err(err());
		}
		Ok((a, b, c))
	}
}
