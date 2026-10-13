//! `sql` -> `expr` conversions for [`crate::sql::algorithm`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::algorithm::*;

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
