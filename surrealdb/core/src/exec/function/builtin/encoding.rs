//! Encoding functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

// Base64 encoding
define_pure_function!(EncodingBase64Decode, "encoding::base64::decode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> Any, crate::fnc::encoding::base64::decode);
define_pure_function!(EncodingBase64Encode, "encoding::base64::encode", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Bytes, ?padded: Bool) -> String, crate::fnc::encoding::base64::encode);

// CBOR encoding
define_pure_function!(EncodingCborDecode, "encoding::cbor::decode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::encoding::cbor::decode);
define_pure_function!(EncodingCborEncode, "encoding::cbor::encode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::encoding::cbor::encode);

// JSON encoding
define_pure_function!(EncodingJsonDecode, "encoding::json::decode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> Any, crate::fnc::encoding::json::decode);
define_pure_function!(EncodingJsonEncode, "encoding::json::encode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::encoding::json::encode);

/// The `encoding::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&EncodingBase64Decode,
	&EncodingBase64Encode,
	&EncodingCborDecode,
	&EncodingCborEncode,
	&EncodingJsonDecode,
	&EncodingJsonEncode,
];
