//! Encoding functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// Base64 encoding
define_pure_function!(EncodingBase64Decode, "encoding::base64::decode", (value: String) -> Any, crate::fnc::encoding::base64::decode);
define_pure_function!(EncodingBase64Encode, "encoding::base64::encode", (value: Any) -> String, crate::fnc::encoding::base64::encode);

// CBOR encoding
define_pure_function!(EncodingCborDecode, "encoding::cbor::decode", (value: Any) -> Any, crate::fnc::encoding::cbor::decode);
define_pure_function!(EncodingCborEncode, "encoding::cbor::encode", (value: Any) -> Any, crate::fnc::encoding::cbor::encode);

// JSON encoding
define_pure_function!(EncodingJsonDecode, "encoding::json::decode", (value: String) -> Any, crate::fnc::encoding::json::decode);
define_pure_function!(EncodingJsonEncode, "encoding::json::encode", (value: Any) -> String, crate::fnc::encoding::json::encode);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		EncodingBase64Decode,
		EncodingBase64Encode,
		EncodingCborDecode,
		EncodingCborEncode,
		EncodingJsonDecode,
		EncodingJsonEncode,
	);
}
