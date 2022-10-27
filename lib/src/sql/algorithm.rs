use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
		map(tag("EDDSA"), |_| Algorithm::EdDSA),
		map(tag("ES256"), |_| Algorithm::Es256),
		map(tag("ES384"), |_| Algorithm::Es384),
		map(tag("ES512"), |_| Algorithm::Es512),
		map(tag("HS256"), |_| Algorithm::Hs256),
		map(tag("HS384"), |_| Algorithm::Hs384),
		map(tag("HS512"), |_| Algorithm::Hs512),
		map(tag("PS256"), |_| Algorithm::Ps256),
		map(tag("PS384"), |_| Algorithm::Ps384),
		map(tag("PS512"), |_| Algorithm::Ps512),
		map(tag("RS256"), |_| Algorithm::Rs256),
		map(tag("RS384"), |_| Algorithm::Rs384),
		map(tag("RS512"), |_| Algorithm::Rs512),
	))(i)
}
