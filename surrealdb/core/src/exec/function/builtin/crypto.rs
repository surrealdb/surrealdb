//! Crypto functions (synchronous hash functions only)
//!
//! Note: Async crypto functions (argon2, bcrypt, pbkdf2, scrypt) are not included
//! here as they require async execution context.

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(CryptoBlake3, "crypto::blake3", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::crypto::blake3);
define_pure_function!(CryptoJoaat, "crypto::joaat", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::crypto::joaat);
define_pure_function!(CryptoMd5, "crypto::md5", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::crypto::md5);
define_pure_function!(CryptoSha1, "crypto::sha1", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::crypto::sha1);
define_pure_function!(CryptoSha256, "crypto::sha256", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::crypto::sha256);
define_pure_function!(CryptoSha512, "crypto::sha512", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::crypto::sha512);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		CryptoBlake3,
		CryptoJoaat,
		CryptoMd5,
		CryptoSha1,
		CryptoSha256,
		CryptoSha512,
	);
}
