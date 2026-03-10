use crate::expr::{Expr, Literal};
use crate::val::Duration;

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
// Durations representing the expiration of different elements of the access method
// In this context, the None variant represents that the element does not expire
pub(crate) struct AccessDuration {
	// Duration after which the grants generated with the access method expire
	// For access methods whose grants are tokens, this value is irrelevant
	pub(crate) grant: Expr,
	// Duration after which the tokens obtained with the access method expire
	// For access methods that cannot issue tokens, this value is irrelevant
	pub(crate) token: Expr,
	// Duration after which the session authenticated with the access method expires
	pub(crate) session: Expr,
}

impl Default for AccessDuration {
	fn default() -> Self {
		Self {
			// By default, access grants expire in 30 days.
			grant: Expr::Literal(Literal::Duration(
				Duration::from_days(30).expect("30 days should fit in a duration"),
			)),
			// By default, tokens expire after one hour
			token: Expr::Literal(Literal::Duration(
				Duration::from_hours(1).expect("1 hour should fit in a duration"),
			)),
			// By default, sessions do not expire
			session: Expr::Literal(Literal::None),
		}
	}
}
