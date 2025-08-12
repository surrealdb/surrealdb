use anyhow::{Result, bail};
use chrono::{Duration as ChronoDuration, Utc};
use jsonwebtoken::EncodingKey;

use crate::err::Error;
use crate::expr::Algorithm;
use crate::val::Duration;

pub(crate) fn config(alg: Algorithm, key: &str) -> Result<EncodingKey> {
	match alg {
		Algorithm::Hs256 => Ok(EncodingKey::from_secret(key.as_ref())),
		Algorithm::Hs384 => Ok(EncodingKey::from_secret(key.as_ref())),
		Algorithm::Hs512 => Ok(EncodingKey::from_secret(key.as_ref())),
		Algorithm::EdDSA => Ok(EncodingKey::from_ed_pem(key.as_ref())?),
		Algorithm::Es256 => Ok(EncodingKey::from_ec_pem(key.as_ref())?),
		Algorithm::Es384 => Ok(EncodingKey::from_ec_pem(key.as_ref())?),
		Algorithm::Es512 => Ok(EncodingKey::from_ec_pem(key.as_ref())?),
		Algorithm::Ps256 => Ok(EncodingKey::from_rsa_pem(key.as_ref())?),
		Algorithm::Ps384 => Ok(EncodingKey::from_rsa_pem(key.as_ref())?),
		Algorithm::Ps512 => Ok(EncodingKey::from_rsa_pem(key.as_ref())?),
		Algorithm::Rs256 => Ok(EncodingKey::from_rsa_pem(key.as_ref())?),
		Algorithm::Rs384 => Ok(EncodingKey::from_rsa_pem(key.as_ref())?),
		Algorithm::Rs512 => Ok(EncodingKey::from_rsa_pem(key.as_ref())?),
	}
}

pub(crate) fn expiration(d: Option<Duration>) -> Result<Option<i64>> {
	let exp = match d {
		Some(v) => {
			// The defined duration must be valid
			match ChronoDuration::from_std(v.0) {
				// The resulting expiration must be valid
				Ok(d) => match Utc::now().checked_add_signed(d) {
					Some(exp) => Some(exp.timestamp()),
					None => bail!(Error::AccessInvalidExpiration),
				},
				Err(_) => bail!(Error::AccessInvalidDuration),
			}
		}
		_ => None,
	};

	Ok(exp)
}
