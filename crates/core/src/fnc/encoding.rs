pub mod base64 {
	use crate::err::Error;
	use crate::sql::{Bytes, Value};
	use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};

	pub fn encode((arg,): (Bytes,)) -> Result<Value, Error> {
		Ok(Value::from(STANDARD_NO_PAD.encode(&*arg)))
	}

	pub fn decode((arg,): (String,)) -> Result<Value, Error> {
		Ok(Value::from(Bytes(STANDARD_NO_PAD.decode(arg).map_err(|_| {
			Error::InvalidArguments {
				name: "encoding::base64::decode".to_owned(),
				message: "invalid base64".to_owned(),
			}
		})?)))
	}
}
