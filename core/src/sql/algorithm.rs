use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Algorithm {
	EdDSA,
	Es256,
	Es384,
	Es512,
	Hs256,
	Hs384,
	Hs512,
	Ps256,
	Ps384,
	Ps512,
	Rs256,
	Rs384,
	Rs512,
}

impl Default for Algorithm {
	fn default() -> Self {
		Self::Hs512
	}
}

impl fmt::Display for Algorithm {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::EdDSA => "EDDSA",
			Self::Es256 => "ES256",
			Self::Es384 => "ES384",
			Self::Es512 => "ES512",
			Self::Hs256 => "HS256",
			Self::Hs384 => "HS384",
			Self::Hs512 => "HS512",
			Self::Ps256 => "PS256",
			Self::Ps384 => "PS384",
			Self::Ps512 => "PS512",
			Self::Rs256 => "RS256",
			Self::Rs384 => "RS384",
			Self::Rs512 => "RS512",
		})
	}
}
