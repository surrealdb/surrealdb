use super::super::IResult;
use crate::sql::Algorithm;
use nom::{branch::alt, bytes::complete::tag, combinator::value};

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
		value(Algorithm::Jwks, tag("JWKS")), // Not an algorithm.
	))(i)
}
