use crate::err::Error;
use std::fmt;
use std::io;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[non_exhaustive]
pub enum DoneReason {
	Timedout,
	Canceled,
}

impl fmt::Display for DoneReason {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			DoneReason::Timedout => write!(f, "Context timedout"),
			DoneReason::Canceled => write!(f, "Context canceled"),
		}
	}
}

impl From<DoneReason> for Error {
	fn from(reason: DoneReason) -> Self {
		match reason {
			DoneReason::Timedout => Error::QueryTimedout,
			DoneReason::Canceled => Error::QueryCancelled,
		}
	}
}

impl From<DoneReason> for io::Error {
	fn from(reason: DoneReason) -> Self {
		let kind = match reason {
			DoneReason::Timedout => io::ErrorKind::TimedOut,
			DoneReason::Canceled => io::ErrorKind::Other,
		};
		io::Error::new(kind, reason.to_string())
	}
}
