//! The `crypto::*` functions, from `surrealdb-runtime`.
//!
//! A family whose cargo feature is off is absent from the runtime crate; the
//! stand-ins here keep every `fnc::crypto::…` path resolving and fail each
//! call with an error naming the feature.

pub use surrealdb_runtime::crypto::*;

#[cfg(not(feature = "crypto-digest"))]
super::feature::disabled_functions! {
	"crypto-digest";
	blake3((String,)) => "crypto::blake3";
	md5((String,)) => "crypto::md5";
	sha1((String,)) => "crypto::sha1";
	sha256((String,)) => "crypto::sha256";
	sha512((String,)) => "crypto::sha512";
}

/// Declares the stand-in `cmp` and `gen` of one password-hash family.
#[cfg(not(feature = "crypto-password"))]
macro_rules! disabled_password_hash {
	($($family:ident),+) => {
		$(
			pub mod $family {
				crate::fnc::feature::disabled_functions! {
					"crypto-password";
					cmp((String, String)) => concat!("crypto::", stringify!($family), "::compare");
					r#gen((String,)) => concat!("crypto::", stringify!($family), "::generate");
				}
			}
		)+
	};
}

#[cfg(not(feature = "crypto-password"))]
disabled_password_hash!(argon2, bcrypt, pbkdf2, scrypt);
