pub mod base64 {
	use anyhow::Result;
	use base64::engine::DecodePaddingMode;
	use base64::engine::general_purpose::{
		GeneralPurpose, GeneralPurposeConfig, STANDARD, STANDARD_NO_PAD,
	};
	use base64::{Engine, alphabet};

	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::val::{Bytes, Value};

	/// Base64 configuration which supports decoding with or without padding.
	const STANDARD_GENERIC_DECODER: GeneralPurpose = GeneralPurpose::new(
		&alphabet::STANDARD,
		GeneralPurposeConfig::new()
			.with_encode_padding(false)
			.with_decode_padding_mode(DecodePaddingMode::Indifferent),
	);

	/// Encodes a `Bytes` value to a base64 string without padding.
	pub fn encode((arg, Optional(padded)): (Bytes, Optional<bool>)) -> Result<Value> {
		let padded = padded.unwrap_or_default();
		let engine = if padded {
			STANDARD
		} else {
			STANDARD_NO_PAD
		};
		Ok(Value::from(engine.encode(&*arg)))
	}

	/// Decodes a base64 string to a `Bytes` value. It accepts both padded and
	/// non-padded base64 strings.
	pub fn decode((arg,): (String,)) -> Result<Value> {
		Ok(Value::from(Bytes(STANDARD_GENERIC_DECODER.decode(arg).map_err(|_| {
			Error::InvalidArguments {
				name: "encoding::base64::decode".to_owned(),
				message: "invalid base64".to_owned(),
			}
		})?)))
	}
}
pub mod cbor {
	use anyhow::Result;

	use crate::err::Error;
	use crate::rpc::format::cbor;
	use crate::val::{Bytes, Value};

	pub fn encode((arg,): (Value,)) -> Result<Value> {
		let val = cbor::encode(arg).map_err(|_| Error::InvalidArguments {
			name: "encoding::cbor::encode".to_owned(),
			message: "Value could not be encoded into CBOR".to_owned(),
		})?;

		Ok(Value::Bytes(Bytes(val)))
	}

	pub fn decode((arg,): (Bytes,)) -> Result<Value> {
		cbor::decode(arg.as_slice())
			.map_err(|_| Error::InvalidArguments {
				name: "encoding::cbor::decode".to_owned(),
				message: "invalid cbor".to_owned(),
			})
			.map_err(anyhow::Error::new)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::fnc::args::Optional;
	use crate::val::{Bytes, Value};

	#[test]
	fn test_base64_encode() {
		let input = Bytes(b"hello".to_vec());
		let result = base64::encode((input.clone(), Optional(None))).unwrap();
		assert_eq!(result, Value::from("aGVsbG8"));

		let result = base64::encode((input, Optional(Some(false)))).unwrap();
		assert_eq!(result, Value::from("aGVsbG8"));
	}

	#[test]
	fn test_base64_encode_padded() {
		let input = Bytes(b"hello".to_vec());
		let result = base64::encode((input, Optional(Some(true)))).unwrap();
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
