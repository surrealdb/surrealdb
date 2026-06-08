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
	use rand_core::OsRng;

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
		let hash = algo
			.hash_password(pass.as_ref(), &salt)
			.map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?
			.to_string();
		Ok(hash.into())
	}
}

pub mod bcrypt {

	use anyhow::Result;

	use crate::fnc::crypto::COST_ALLOWANCE;
	use crate::val::Value;

	/// Pulls the bcrypt cost out of a `$2X$NN$<salt><hash>` prefix without
	/// allocating. Requires the canonical two-digit `NN` form; `bcrypt::verify`
	/// validates the rest. Returns `None` on any non-canonical input.
	fn extract_cost(hash: &str) -> Option<u32> {
		let bytes = hash.as_bytes();
		if bytes.len() < 7
			|| bytes[0] != b'$'
			|| bytes[1] != b'2'
			|| !matches!(bytes[2], b'a' | b'b' | b'x' | b'y')
			|| bytes[3] != b'$'
			|| bytes[6] != b'$'
		{
			return None;
		}
		let tens = (bytes[4] as char).to_digit(10)?;
		let ones = (bytes[5] as char).to_digit(10)?;
		Some(tens * 10 + ones)
	}

	pub fn cmp((hash, pass): (String, String)) -> Result<Value> {
		let Some(cost) = extract_cost(&hash) else {
			return Ok(Value::Bool(false));
		};
		// Bcrypt cost is exponential, so add the cost allowance instead of
		// multiplying (cf. the Argon2 path above which multiplies).
		if cost > bcrypt::DEFAULT_COST.saturating_add(COST_ALLOWANCE) {
			return Ok(Value::Bool(false));
		}
		Ok(bcrypt::verify(pass, &hash).unwrap_or(false).into())
	}

	pub fn r#gen((pass,): (String,)) -> Result<Value> {
		let hash = bcrypt::hash(pass, bcrypt::DEFAULT_COST)
			.map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?;
		Ok(hash.into())
	}

	#[cfg(test)]
	mod tests {
		use super::{cmp, extract_cost};
		use crate::val::Value;

		#[test]
		fn extract_cost_parses_canonical_hashes() {
			// (version letter, cost field) covering each version the bcrypt
			// crate recognises plus the cost range we care about.
			let suffix = "$ssssssssssssssssssssssHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH";
			for (version, cost) in [('a', 12), ('b', 12), ('x', 12), ('y', 12), ('b', 4), ('b', 31)]
			{
				let hash = format!("$2{version}${cost:02}{suffix}");
				assert_eq!(extract_cost(&hash), Some(cost), "input: {hash}");
			}
		}

		#[test]
		fn extract_cost_rejects_malformed_input() {
			// Wrong version letter.
			assert_eq!(extract_cost("$2c$12$xxx"), None);
			// Missing separators.
			assert_eq!(extract_cost("$2b12$xx"), None);
			// Non-digit cost.
			assert_eq!(extract_cost("$2b$ab$xx"), None);
			// Too short.
			assert_eq!(extract_cost("$2b$12"), None);
			// Empty.
			assert_eq!(extract_cost(""), None);
			// Argon2 hash.
			assert_eq!(extract_cost("$argon2id$v=19$m=65536,t=2,p=1$xx$yy"), None);
		}

		#[test]
		fn extract_cost_requires_canonical_two_digit_cost() {
			// `bcrypt::HashParts::from_str` would accept the single-digit form
			// (it parses with `u32::from_str` after splitting on `$`); we
			// intentionally require canonical `{:02}` to keep the prefix
			// parser branchless. Non-canonical hashes fail closed.
			assert_eq!(
				extract_cost("$2b$4$ssssssssssssssssssssssHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH"),
				None
			);
		}

		#[test]
		fn cmp_roundtrips_a_freshly_generated_hash() {
			// Cost 4 keeps the test fast (~10ms instead of seconds at the default cost).
			let hash = bcrypt::hash("p4ssw0rd", 4).expect("hash generated");
			assert_eq!(cmp((hash.clone(), "p4ssw0rd".into())).unwrap(), Value::Bool(true));
			assert_eq!(cmp((hash, "wrong".into())).unwrap(), Value::Bool(false));
		}

		#[test]
		fn cmp_rejects_a_hash_with_excessive_cost() {
			// Cost 30 would take minutes to verify, so the cap must reject
			// it before bcrypt::verify is reached. The cost-extraction unit
			// tests pin the short-circuit logic; this test pins the
			// observable behaviour.
			let inflated = "$2b$30$ssssssssssssssssssssssHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH";
			assert_eq!(cmp((inflated.into(), "anything".into())).unwrap(), Value::Bool(false));
		}

		#[test]
		fn cmp_rejects_a_malformed_hash() {
			assert_eq!(cmp(("not-a-hash".into(), "x".into())).unwrap(), Value::Bool(false));
			assert_eq!(cmp(("".into(), "x".into())).unwrap(), Value::Bool(false));
		}
	}
}

pub mod pbkdf2 {

	use anyhow::Result;
	use pbkdf2::Pbkdf2;
	use pbkdf2::password_hash::{PasswordHash, PasswordHasher, SaltString};
	use rand_core::OsRng;

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
		let hash = Pbkdf2
			.hash_password(pass.as_ref(), &salt)
			.map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?
			.to_string();
		Ok(hash.into())
	}
}

pub mod scrypt {

	use anyhow::Result;
	use rand_core::OsRng;
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
		let hash = Scrypt
			.hash_password(pass.as_ref(), &salt)
			.map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?
			.to_string();
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
