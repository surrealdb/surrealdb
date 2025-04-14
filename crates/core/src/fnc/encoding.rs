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

pub mod cbor {
	use crate::err::Error;
	use crate::rpc::format::cbor::Cbor;
	use crate::sql::{Bytes, Value};
	use ciborium::Value as Data;

	pub fn encode((arg,): (Value,)) -> Result<Value, Error> {
		let val: Cbor = arg.try_into().map_err(|_| Error::InvalidArguments {
			name: "encoding::cbor::encode".to_owned(),
			message: "Value could not be encoded into CBOR".to_owned(),
		})?;

		// Create a new vector for encoding output
		let mut res = Vec::new();
		// Serialize the value into CBOR binary data
		ciborium::into_writer(&val.0, &mut res).map_err(|_| Error::InvalidArguments {
			name: "encoding::cbor::encode".to_owned(),
			message: "Value could not be encoded into CBOR".to_owned(),
		})?;

		Ok(Value::Bytes(Bytes(res)))
	}

	pub fn decode((arg,): (Bytes,)) -> Result<Value, Error> {
		let cbor = ciborium::from_reader::<Data, _>(&mut arg.as_slice())
			.map_err(|_| Error::InvalidArguments {
				name: "encoding::cbor::decode".to_owned(),
				message: "invalid cbor".to_owned(),
			})
			.map(Cbor)?;

		Value::try_from(cbor).map_err(|v: &str| Error::InvalidArguments {
			name: "encoding::cbor::decode".to_owned(),
			message: v.to_owned(),
		})
	}
}
