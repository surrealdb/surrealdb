use crate::{Expr, Literal};

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// Durations representing the expiration of different elements of the access
// method In this context, the None variant represents that the element does not
// expire
pub struct AccessDuration {
	// Duration after which the grants generated with the access method expire
	// For access methods whose grants are tokens, this value is irrelevant
	pub grant: Expr,
	// Duration after which the tokens obtained with the access method expire
	// For access methods that cannot issue tokens, this value is irrelevant
	pub token: Expr,
	// Duration after which the session authenticated with the access method expires
	pub session: Expr,
}

impl Default for AccessDuration {
	fn default() -> Self {
		Self {
			// By default, access grants expire in 30 days.
			grant: Expr::Literal(Literal::Duration(std::time::Duration::from_secs(30 * 86400))),
			// By default, tokens expire after one hour
			token: Expr::Literal(Literal::Duration(std::time::Duration::from_secs(3600))),
			// By default, sessions do not expire
			session: Expr::Literal(Literal::None),
		}
	}
}
