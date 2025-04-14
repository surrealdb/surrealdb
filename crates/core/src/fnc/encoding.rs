pub mod base64 {
	use crate::err::Error;
	use crate::sql::{Bytes, Value};
	use base64::engine::general_purpose::{
		GeneralPurpose, GeneralPurposeConfig, STANDARD, STANDARD_NO_PAD,
	};
	use base64::engine::DecodePaddingMode;
	use base64::{alphabet, Engine};

	/// Base64 configuration which supports decoding with or without padding.
	const STANDARD_GENERIC_DECODER: GeneralPurpose = GeneralPurpose::new(
		&alphabet::STANDARD,
		GeneralPurposeConfig::new()
			.with_encode_padding(false)
			.with_decode_padding_mode(DecodePaddingMode::Indifferent),
	);

	/// Encodes a `Bytes` value to a base64 string without padding.
	pub fn encode((arg,): (Bytes,)) -> Result<Value, Error> {
		Ok(Value::from(STANDARD_NO_PAD.encode(&*arg)))
	}

	/// Encodes a `Bytes` value to a base64 string with padding.
	pub fn encode_padded((arg,): (Bytes,)) -> Result<Value, Error> {
		Ok(Value::from(STANDARD.encode(&*arg)))
	}

	/// Decodes a base64 string to a `Bytes` value. It accepts both padded and
	/// non-padded base64 strings.
	pub fn decode((arg,): (String,)) -> Result<Value, Error> {
		Ok(Value::from(Bytes(STANDARD_GENERIC_DECODER.decode(arg).map_err(|_| {
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

#[cfg(test)]
mod tests {
	use super::*;

	use crate::sql::{Bytes, Value};

	#[test]
	fn test_base64_encode() {
		let input = Bytes(b"hello".to_vec());
		let result = base64::encode((input,)).unwrap();
		assert_eq!(result, Value::from("aGVsbG8"));
	}

	#[test]
	fn test_base64_encode_padded() {
		let input = Bytes(b"hello".to_vec());
		let result = base64::encode_padded((input,)).unwrap();
		assert_eq!(result, Value::from("aGVsbG8="));
	}

	#[test]
	fn test_base64_decode_no_pad() {
		let input = "aGVsbG8".to_string();
		let result = base64::decode((input,)).unwrap();
		assert_eq!(result, Value::from(Bytes(b"hello".to_vec())));
	}

	#[test]
	fn test_base64_decode_with_pad() {
		let input = "aGVsbG8=".to_string();
		let result = base64::decode((input,)).unwrap();
		assert_eq!(result, Value::from(Bytes(b"hello".to_vec())));
	}
}
