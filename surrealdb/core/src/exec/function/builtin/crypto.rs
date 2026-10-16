//! Synchronous crypto functions: the hash functions that run on the calling thread.
//!
//! The async functions (argon2, bcrypt, pbkdf2, scrypt) are not defined here
//! because they need an async execution context.

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(CryptoBlake3, "crypto::blake3", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::crypto::blake3);
define_pure_function!(CryptoJoaat, "crypto::joaat", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::crypto::joaat);
define_pure_function!(CryptoMd5, "crypto::md5", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::crypto::md5);
define_pure_function!(CryptoSha1, "crypto::sha1", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::crypto::sha1);
define_pure_function!(CryptoSha256, "crypto::sha256", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::crypto::sha256);
define_pure_function!(CryptoSha512, "crypto::sha512", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::crypto::sha512);

/// The `crypto::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] =
	&[&CryptoBlake3, &CryptoJoaat, &CryptoMd5, &CryptoSha1, &CryptoSha256, &CryptoSha512];
