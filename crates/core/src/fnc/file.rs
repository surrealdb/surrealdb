use crate::{
	err::Error,
	sql::{File, Value},
};

pub fn bucket((file,): (File,)) -> Result<Value, Error> {
	Ok(file.bucket.into())
}

pub fn key((file,): (File,)) -> Result<Value, Error> {
	Ok(file.key.into())
}
