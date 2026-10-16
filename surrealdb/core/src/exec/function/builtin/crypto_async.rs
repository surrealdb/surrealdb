//! Async crypto functions for the engine.
//!
//! These are CPU-intensive crypto functions (argon2, bcrypt, pbkdf2, scrypt)
//! that are offloaded to a blocking thread pool to avoid blocking the async runtime.

use anyhow::Result;

use crate::define_async_function;
use crate::exec::EvalContext;
use crate::exec::function::ScalarFunction;
use crate::fnc::crypto::{argon2, bcrypt, pbkdf2, scrypt};
use crate::val::Value;

// =========================================================================
// Helper to run CPU-intensive work
// =========================================================================

#[cfg(not(target_family = "wasm"))]
async fn cpu_intensive<F, R>(f: F) -> R
where
	F: FnOnce() -> R + Send + 'static,
	R: Send + 'static,
{
	crate::exe::spawn(f).await
}

#[cfg(target_family = "wasm")]
async fn cpu_intensive<F, R>(f: F) -> R
where
	F: FnOnce() -> R + Send + 'static,
	R: Send + 'static,
{
	f()
}

// =========================================================================
// Argon2 functions
// =========================================================================

async fn argon2_compare_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let mut args = args.into_iter();
	let hash = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::argon2::compare' expects a string hash, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::argon2::compare' expects two arguments"
			));
		}
	};
	let pass = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::argon2::compare' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::argon2::compare' expects two arguments"
			));
		}
	};

	cpu_intensive(move || argon2::cmp((hash, pass))).await
}

async fn argon2_generate_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let pass = match args.into_iter().next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::argon2::generate' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::argon2::generate' expects a password argument"
			));
		}
	};

	cpu_intensive(move || argon2::r#gen((pass,))).await
}

// =========================================================================
// Bcrypt functions
// =========================================================================

async fn bcrypt_compare_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let mut args = args.into_iter();
	let hash = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::bcrypt::compare' expects a string hash, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::bcrypt::compare' expects two arguments"
			));
		}
	};
	let pass = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::bcrypt::compare' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::bcrypt::compare' expects two arguments"
			));
		}
	};

	cpu_intensive(move || bcrypt::cmp((hash, pass))).await
}

async fn bcrypt_generate_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let pass = match args.into_iter().next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::bcrypt::generate' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::bcrypt::generate' expects a password argument"
			));
		}
	};

	cpu_intensive(move || bcrypt::r#gen((pass,))).await
}

// =========================================================================
// PBKDF2 functions
// =========================================================================

async fn pbkdf2_compare_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let mut args = args.into_iter();
	let hash = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::pbkdf2::compare' expects a string hash, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::pbkdf2::compare' expects two arguments"
			));
		}
	};
	let pass = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::pbkdf2::compare' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::pbkdf2::compare' expects two arguments"
			));
		}
	};

	cpu_intensive(move || pbkdf2::cmp((hash, pass))).await
}

async fn pbkdf2_generate_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let pass = match args.into_iter().next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::pbkdf2::generate' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::pbkdf2::generate' expects a password argument"
			));
		}
	};

	cpu_intensive(move || pbkdf2::r#gen((pass,))).await
}

// =========================================================================
// Scrypt functions
// =========================================================================

async fn scrypt_compare_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let mut args = args.into_iter();
	let hash = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::scrypt::compare' expects a string hash, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::scrypt::compare' expects two arguments"
			));
		}
	};
	let pass = match args.next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::scrypt::compare' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::scrypt::compare' expects two arguments"
			));
		}
	};

	cpu_intensive(move || scrypt::cmp((hash, pass))).await
}

async fn scrypt_generate_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let pass = match args.into_iter().next() {
		Some(Value::String(s)) => s.into_string(),
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::scrypt::generate' expects a string password, got: {}",
				v.kind_of()
			));
		}
		None => {
			return Err(anyhow::anyhow!(
				"Function 'crypto::scrypt::generate' expects a password argument"
			));
		}
	};

	cpu_intensive(move || scrypt::r#gen((pass,))).await
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_async_function!(CryptoArgon2Compare, "crypto::argon2::compare", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (hash: String, pass: String) -> Bool, argon2_compare_impl);
define_async_function!(CryptoArgon2Generate, "crypto::argon2::generate", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (pass: String) -> String, argon2_generate_impl);
define_async_function!(CryptoBcryptCompare, "crypto::bcrypt::compare", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (hash: String, pass: String) -> Bool, bcrypt_compare_impl);
define_async_function!(CryptoBcryptGenerate, "crypto::bcrypt::generate", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (pass: String) -> String, bcrypt_generate_impl);
define_async_function!(CryptoPbkdf2Compare, "crypto::pbkdf2::compare", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (hash: String, pass: String) -> Bool, pbkdf2_compare_impl);
define_async_function!(CryptoPbkdf2Generate, "crypto::pbkdf2::generate", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (pass: String) -> String, pbkdf2_generate_impl);
define_async_function!(CryptoScryptCompare, "crypto::scrypt::compare", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (hash: String, pass: String) -> Bool, scrypt_compare_impl);
define_async_function!(CryptoScryptGenerate, "crypto::scrypt::generate", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (pass: String) -> String, scrypt_generate_impl);

// =========================================================================
// Registration
// =========================================================================

// Each `generate` draws a fresh salt, so the same password gives a different hash on every call.
// It declares `EffectSet::ENTROPY`, so a rewrite must not copy one call's result into
// several places or fold it into a constant.
// Each `compare` reads the salt from the hash it is given, so it is pure and
// declares `EffectSet::PURE`.
/// The asynchronous `crypto::*` scalar builtins: password hashing and verification.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&CryptoArgon2Compare,
	&CryptoArgon2Generate,
	&CryptoBcryptCompare,
	&CryptoBcryptGenerate,
	&CryptoPbkdf2Compare,
	&CryptoPbkdf2Generate,
	&CryptoScryptCompare,
	&CryptoScryptGenerate,
];
