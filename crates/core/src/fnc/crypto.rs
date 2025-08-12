use anyhow::Result;
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::{Sha256, Sha512};

use crate::val::Value;

pub fn blake3((arg,): (String,)) -> Result<Value> {
	Ok(blake3::hash(arg.as_bytes()).to_string().into())
}

pub fn joaat((arg,): (String,)) -> Result<Value> {
	Ok(joaat::hash_bytes(arg.as_bytes()).into())
}

pub fn md5((arg,): (String,)) -> Result<Value> {
	let mut hasher = Md5::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn sha1((arg,): (String,)) -> Result<Value> {
	let mut hasher = Sha1::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn sha256((arg,): (String,)) -> Result<Value> {
	let mut hasher = Sha256::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn sha512((arg,): (String,)) -> Result<Value> {
	let mut hasher = Sha512::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

/// Allowed to cost this much more than default setting for each hash function.
const COST_ALLOWANCE: u32 = 4;

/// Like verify_password, but takes a closure to determine whether the cost of
/// performing the operation is not too high.
macro_rules! bounded_verify_password {
	($algo: ident, $instance: expr_2021, $password: expr_2021, $hash: expr_2021, $bound: expr_2021) => {
		if let (Some(salt), Some(expected_output)) = (&$hash.salt, &$hash.hash) {
			if let Some(params) =
				<$algo as PasswordHasher>::Params::try_from($hash).ok().filter($bound)
			{
				if let Ok(computed_hash) = $instance.hash_password_customized(
					$password.as_ref(),
					Some($hash.algorithm),
					$hash.version,
					params,
					*salt,
				) {
					if let Some(computed_output) = &computed_hash.hash {
						expected_output == computed_output
					} else {
						false
					}
				} else {
					false
				}
			} else {
				false
			}
		} else {
			false
		}
	};

	($algo: ident, $password: expr_2021, $hash: expr_2021, $bound: expr_2021) => {
		bounded_verify_password!($algo, $algo::default(), $password, $hash, $bound)
	};
}

pub mod argon2 {

	use anyhow::Result;
	use argon2::Argon2;
	use argon2::password_hash::{PasswordHash, PasswordHasher, SaltString};
	use rand::rngs::OsRng;

	use super::COST_ALLOWANCE;
	use crate::val::Value;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value> {
		type Params<'a> = <Argon2<'a> as PasswordHasher>::Params;
		Ok(PasswordHash::new(&hash)
			.ok()
			.filter(|test| {
				bounded_verify_password!(Argon2, pass, test, |params: &Params| {
					params.m_cost() <= Params::DEFAULT_M_COST.saturating_mul(COST_ALLOWANCE)
						&& params.t_cost() <= Params::DEFAULT_T_COST.saturating_mul(COST_ALLOWANCE)
						&& params.p_cost() <= Params::DEFAULT_P_COST.saturating_mul(COST_ALLOWANCE)
				})
			})
			.is_some()
			.into())
	}

	pub fn r#gen((pass,): (String,)) -> Result<Value> {
		let algo = Argon2::default();
		let salt = SaltString::generate(&mut OsRng);
		let hash = algo.hash_password(pass.as_ref(), &salt).unwrap().to_string();
		Ok(hash.into())
	}
}

pub mod bcrypt {

	use std::str::FromStr;

	use anyhow::Result;
	use bcrypt::HashParts;

	use crate::fnc::crypto::COST_ALLOWANCE;
	use crate::val::Value;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value> {
		let parts = match HashParts::from_str(&hash) {
			Ok(parts) => parts,
			Err(_) => return Ok(Value::Bool(false)),
		};
		// Note: Bcrypt cost is exponential, so add the cost allowance as opposed to
		// multiplying.
		Ok(if parts.get_cost() > bcrypt::DEFAULT_COST.saturating_add(COST_ALLOWANCE) {
			// Too expensive to compute.
			Value::Bool(false)
		} else {
			// FIXME: If base64 dependency is added, can avoid parsing the HashParts twice,
			// once above and once in verity, by using bcrypt::bcrypt.
			bcrypt::verify(pass, &hash).unwrap_or(false).into()
		})
	}

	pub fn r#gen((pass,): (String,)) -> Result<Value> {
		let hash = bcrypt::hash(pass, bcrypt::DEFAULT_COST).unwrap();
		Ok(hash.into())
	}
}

pub mod pbkdf2 {

	use anyhow::Result;
	use pbkdf2::Pbkdf2;
	use pbkdf2::password_hash::{PasswordHash, PasswordHasher, SaltString};
	use rand::rngs::OsRng;

	use super::COST_ALLOWANCE;
	use crate::val::Value;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value> {
		type Params = <Pbkdf2 as PasswordHasher>::Params;
		Ok(PasswordHash::new(&hash)
			.ok()
			.filter(|test| {
				bounded_verify_password!(Pbkdf2, Pbkdf2, pass, test, |params: &Params| {
					params.rounds <= Params::default().rounds.saturating_mul(COST_ALLOWANCE)
						&& params.output_length
							<= Params::default()
								.output_length
								.saturating_mul(COST_ALLOWANCE as usize)
				})
			})
			.is_some()
			.into())
	}

	pub fn r#gen((pass,): (String,)) -> Result<Value> {
		let salt = SaltString::generate(&mut OsRng);
		let hash = Pbkdf2.hash_password(pass.as_ref(), &salt).unwrap().to_string();
		Ok(hash.into())
	}
}

pub mod scrypt {

	use anyhow::Result;
	use rand::rngs::OsRng;
	use scrypt::Scrypt;
	use scrypt::password_hash::{PasswordHash, PasswordHasher, SaltString};

	use crate::val::Value;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value> {
		type Params = <Scrypt as PasswordHasher>::Params;
		Ok(PasswordHash::new(&hash)
			.ok()
			.filter(|test| {
				bounded_verify_password!(Scrypt, Scrypt, pass, test, |params: &Params| {
					// Scrypt is slow, use lower cost allowance.
					// Also note that the log_n parameter behaves exponentially, so add instead
					// of multiplying.
					params.log_n() <= Params::default().log_n().saturating_add(2)
						&& params.r() <= Params::default().r().saturating_mul(2)
						&& params.p() <= Params::default().p().saturating_mul(4)
				})
			})
			.is_some()
			.into())
	}

	pub fn r#gen((pass,): (String,)) -> Result<Value> {
		let salt = SaltString::generate(&mut OsRng);
		let hash = Scrypt.hash_password(pass.as_ref(), &salt).unwrap().to_string();
		Ok(hash.into())
	}
}

/// Code borrowed from [joaat-rs](https://github.com/Pocakking/joaat-rs).
/// All credits to its author.
mod joaat {
	use std::default::Default;
	use std::hash::Hasher;

	pub struct JoaatHasher(u32);

	impl Default for JoaatHasher {
		#[inline]
		fn default() -> Self {
			Self(0)
		}
	}

	impl Hasher for JoaatHasher {
		#[inline]
		fn finish(&self) -> u64 {
			let mut hash = self.0;
			hash = hash.wrapping_add(hash.wrapping_shl(3));
			hash ^= hash.wrapping_shr(11);
			hash = hash.wrapping_add(hash.wrapping_shl(15));
			hash as _
		}

		#[inline]
		fn write(&mut self, bytes: &[u8]) {
			for byte in bytes.iter() {
				self.0 = self.0.wrapping_add(u32::from(*byte));
				self.0 = self.0.wrapping_add(self.0.wrapping_shl(10));
				self.0 ^= self.0.wrapping_shr(6);
			}
		}
	}

	/// Hashes a slice of bytes.
	#[inline]
	#[must_use]
	pub fn hash_bytes(bytes: &[u8]) -> u32 {
		let mut hasher = JoaatHasher::default();
		hasher.write(bytes);
		hasher.finish() as _
	}

	#[cfg(test)]
	#[allow(clippy::unreadable_literal)]
	mod tests {
		use super::*;

		#[test]
		fn test() {
			assert_eq!(hash_bytes(b""), 0);
			assert_eq!(hash_bytes(b"a"), 0xCA2E9442);
			assert_eq!(hash_bytes(b"b"), 0x00DB819B);
			assert_eq!(hash_bytes(b"c"), 0xEEBA5D59);
			assert_eq!(hash_bytes(b"The quick brown fox jumps over the lazy dog"), 0x519E91F5);
			assert_eq!(hash_bytes(b"The quick brown fox jumps over the lazy dog."), 0xAE8EF3CB);
		}
	}
}
