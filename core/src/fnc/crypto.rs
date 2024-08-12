use crate::err::Error;
use crate::sql::value::Value;
use md5::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::Sha256;
use sha2::Sha512;

pub fn md5((arg,): (String,)) -> Result<Value, Error> {
	let mut hasher = Md5::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn sha1((arg,): (String,)) -> Result<Value, Error> {
	let mut hasher = Sha1::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn sha256((arg,): (String,)) -> Result<Value, Error> {
	let mut hasher = Sha256::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn sha512((arg,): (String,)) -> Result<Value, Error> {
	let mut hasher = Sha512::new();
	hasher.update(arg.as_str());
	let val = hasher.finalize();
	let val = format!("{val:x}");
	Ok(val.into())
}

pub fn blake3((arg,): (String,)) -> Result<Value, Error> {
	Ok(blake3::hash(arg.as_bytes()).to_string().into())
}

/// Allowed to cost this much more than default setting for each hash function.
const COST_ALLOWANCE: u32 = 4;

/// Like verify_password, but takes a closure to determine whether the cost of performing the
/// operation is not too high.
macro_rules! bounded_verify_password {
	($algo: ident, $instance: expr, $password: expr, $hash: expr, $bound: expr) => {
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

	($algo: ident, $password: expr, $hash: expr, $bound: expr) => {
		bounded_verify_password!($algo, $algo::default(), $password, $hash, $bound)
	};
}

pub mod argon2 {

	use super::COST_ALLOWANCE;
	use crate::err::Error;
	use crate::sql::value::Value;
	use argon2::{
		password_hash::{PasswordHash, PasswordHasher, SaltString},
		Argon2,
	};
	use rand::rngs::OsRng;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value, Error> {
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

	pub fn gen((pass,): (String,)) -> Result<Value, Error> {
		let algo = Argon2::default();
		let salt = SaltString::generate(&mut OsRng);
		let hash = algo.hash_password(pass.as_ref(), &salt).unwrap().to_string();
		Ok(hash.into())
	}
}

pub mod bcrypt {

	use crate::err::Error;
	use crate::fnc::crypto::COST_ALLOWANCE;
	use crate::sql::value::Value;
	use bcrypt::HashParts;
	use std::str::FromStr;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value, Error> {
		let parts = match HashParts::from_str(&hash) {
			Ok(parts) => parts,
			Err(_) => return Ok(Value::Bool(false)),
		};
		// Note: Bcrypt cost is exponential, so add the cost allowance as opposed to multiplying.
		Ok(if parts.get_cost() > bcrypt::DEFAULT_COST.saturating_add(COST_ALLOWANCE) {
			// Too expensive to compute.
			Value::Bool(false)
		} else {
			// FIXME: If base64 dependency is added, can avoid parsing the HashParts twice, once
			// above and once in verity, by using bcrypt::bcrypt.
			bcrypt::verify(pass, &hash).unwrap_or(false).into()
		})
	}

	pub fn gen((pass,): (String,)) -> Result<Value, Error> {
		let hash = bcrypt::hash(pass, bcrypt::DEFAULT_COST).unwrap();
		Ok(hash.into())
	}
}

pub mod pbkdf2 {

	use super::COST_ALLOWANCE;
	use crate::err::Error;
	use crate::sql::value::Value;
	use pbkdf2::{
		password_hash::{PasswordHash, PasswordHasher, SaltString},
		Pbkdf2,
	};
	use rand::rngs::OsRng;

	pub fn cmp((hash, pass): (String, String)) -> Result<Value, Error> {
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

	pub fn gen((pass,): (String,)) -> Result<Value, Error> {
		let salt = SaltString::generate(&mut OsRng);
		let hash = Pbkdf2.hash_password(pass.as_ref(), &salt).unwrap().to_string();
		Ok(hash.into())
	}
}

pub mod scrypt {

	use crate::err::Error;
	use crate::sql::value::Value;
	use rand::rngs::OsRng;
	use scrypt::{
		password_hash::{PasswordHash, PasswordHasher, SaltString},
		Scrypt,
	};

	pub fn cmp((hash, pass): (String, String)) -> Result<Value, Error> {
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

	pub fn gen((pass,): (String,)) -> Result<Value, Error> {
		let salt = SaltString::generate(&mut OsRng);
		let hash = Scrypt.hash_password(pass.as_ref(), &salt).unwrap().to_string();
		Ok(hash.into())
	}
}
