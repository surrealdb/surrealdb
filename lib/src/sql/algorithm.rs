use crate::sql::error::IResult;
use nom::bytes::complete::tag;
use nom::{branch::alt, combinator::value};
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

pub fn algorithm(i: &str) -> IResult<&str, Algorithm> {
	alt((
		value(Algorithm::EdDSA, tag("EDDSA")),
		value(Algorithm::Es256, tag("ES256")),
		value(Algorithm::Es384, tag("ES384")),
		value(Algorithm::Es512, tag("ES512")),
		value(Algorithm::Hs256, tag("HS256")),
		value(Algorithm::Hs384, tag("HS384")),
		value(Algorithm::Hs512, tag("HS512")),
		value(Algorithm::Ps256, tag("PS256")),
		value(Algorithm::Ps384, tag("PS384")),
		value(Algorithm::Ps512, tag("PS512")),
		value(Algorithm::Rs256, tag("RS256")),
		value(Algorithm::Rs384, tag("RS384")),
		value(Algorithm::Rs512, tag("RS512")),
	))(i)
}
