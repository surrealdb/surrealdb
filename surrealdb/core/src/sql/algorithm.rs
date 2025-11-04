use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl Algorithm {
	// Does the algorithm use the same key for signing and verification?
	pub(crate) fn is_symmetric(self) -> bool {
		matches!(self, Algorithm::Hs256 | Algorithm::Hs384 | Algorithm::Hs512)
	}
}

impl From<Algorithm> for jsonwebtoken::Algorithm {
	fn from(val: Algorithm) -> Self {
		match val {
			Algorithm::Hs256 => jsonwebtoken::Algorithm::HS256,
			Algorithm::Hs384 => jsonwebtoken::Algorithm::HS384,
			Algorithm::Hs512 => jsonwebtoken::Algorithm::HS512,
			Algorithm::EdDSA => jsonwebtoken::Algorithm::EdDSA,
			Algorithm::Es256 => jsonwebtoken::Algorithm::ES256,
			Algorithm::Es384 => jsonwebtoken::Algorithm::ES384,
			Algorithm::Es512 => jsonwebtoken::Algorithm::ES384,
			Algorithm::Ps256 => jsonwebtoken::Algorithm::PS256,
			Algorithm::Ps384 => jsonwebtoken::Algorithm::PS384,
			Algorithm::Ps512 => jsonwebtoken::Algorithm::PS512,
			Algorithm::Rs256 => jsonwebtoken::Algorithm::RS256,
			Algorithm::Rs384 => jsonwebtoken::Algorithm::RS384,
			Algorithm::Rs512 => jsonwebtoken::Algorithm::RS512,
		}
	}
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

impl From<Algorithm> for crate::expr::Algorithm {
	fn from(v: Algorithm) -> Self {
		match v {
			Algorithm::EdDSA => Self::EdDSA,
			Algorithm::Es256 => Self::Es256,
			Algorithm::Es384 => Self::Es384,
			Algorithm::Es512 => Self::Es512,
			Algorithm::Hs256 => Self::Hs256,
			Algorithm::Hs384 => Self::Hs384,
			Algorithm::Hs512 => Self::Hs512,
			Algorithm::Ps256 => Self::Ps256,
			Algorithm::Ps384 => Self::Ps384,
			Algorithm::Ps512 => Self::Ps512,
			Algorithm::Rs256 => Self::Rs256,
			Algorithm::Rs384 => Self::Rs384,
			Algorithm::Rs512 => Self::Rs512,
		}
	}
}

impl From<crate::expr::Algorithm> for Algorithm {
	fn from(v: crate::expr::Algorithm) -> Self {
		match v {
			crate::expr::Algorithm::EdDSA => Self::EdDSA,
			crate::expr::Algorithm::Es256 => Self::Es256,
			crate::expr::Algorithm::Es384 => Self::Es384,
			crate::expr::Algorithm::Es512 => Self::Es512,
			crate::expr::Algorithm::Hs256 => Self::Hs256,
			crate::expr::Algorithm::Hs384 => Self::Hs384,
			crate::expr::Algorithm::Hs512 => Self::Hs512,
			crate::expr::Algorithm::Ps256 => Self::Ps256,
			crate::expr::Algorithm::Ps384 => Self::Ps384,
			crate::expr::Algorithm::Ps512 => Self::Ps512,
			crate::expr::Algorithm::Rs256 => Self::Rs256,
			crate::expr::Algorithm::Rs384 => Self::Rs384,
			crate::expr::Algorithm::Rs512 => Self::Rs512,
		}
	}
}
