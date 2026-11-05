use std::{fmt, io};

use crate::err::{EngineError, Error};
use crate::val::Duration;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Reason {
	Timedout(Duration),
	Canceled,
}

impl fmt::Display for Reason {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Reason::Timedout(d) => write!(f, "Context timedout: {d:?}"),
			Reason::Canceled => write!(f, "Context canceled"),
		}
	}
}

impl From<Reason> for Error {
	fn from(reason: Reason) -> Self {
		match reason {
			Reason::Timedout(d) => EngineError::QueryTimedout(d.0).into(),
			Reason::Canceled => EngineError::QueryCancelled.into(),
		}
	}
}

impl From<Reason> for io::Error {
	fn from(reason: Reason) -> Self {
		let kind = match reason {
			Reason::Timedout(_) => io::ErrorKind::TimedOut,
			Reason::Canceled => io::ErrorKind::Other,
		};
		io::Error::new(kind, reason.to_string())
	}
}
