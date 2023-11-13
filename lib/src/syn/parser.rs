use crate::err::Error;
use crate::sql::{Datetime, Duration, Idiom, Query, Range, Subquery, Thing, Value};

pub fn subquery(i: &str) -> Result<Subquery, Error> {
	todo!()
}

pub fn value(i: &str) -> Result<Value, Error> {
	todo!()
}

pub fn idiom(i: &str) -> Result<Idiom, Error> {
	todo!()
}

pub fn json(i: &str) -> Result<Value, Error> {
	todo!()
}

pub fn parse(i: &str) -> Result<Query, Error> {
	todo!()
}

pub fn datetime(i: &str) -> Result<Datetime, Error> {
	todo!()
}

pub fn datetime_raw(i: &str) -> Result<Datetime, Error> {
	todo!()
}

pub fn duration(i: &str) -> Result<Duration, Error> {
	todo!()
}

pub fn range(i: &str) -> Result<Range, Error> {
	todo!()
}

pub fn thing(i: &str) -> Result<Thing, Error> {
	todo!()
}

pub fn thing_raw(i: &str) -> Result<Thing, Error> {
	todo!()
}
