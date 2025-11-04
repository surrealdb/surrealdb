use std::{fmt, io};

use crate::err::Error;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Reason {
	Timedout,
	Canceled,
}

impl fmt::Display for Reason {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Reason::Timedout => write!(f, "Context timedout"),
			Reason::Canceled => write!(f, "Context canceled"),
		}
	}
}

impl From<Reason> for Error {
	fn from(reason: Reason) -> Self {
		match reason {
			Reason::Timedout => Error::QueryTimedout,
			Reason::Canceled => Error::QueryCancelled,
		}
	}
}

impl From<Reason> for io::Error {
	fn from(reason: Reason) -> Self {
		let kind = match reason {
			Reason::Timedout => io::ErrorKind::TimedOut,
			Reason::Canceled => io::ErrorKind::Other,
		};
		io::Error::new(kind, reason.to_string())
	}
}
