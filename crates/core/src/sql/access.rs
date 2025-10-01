use crate::sql::{Expr, Literal};
use crate::types::PublicDuration;

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// Durations representing the expiration of different elements of the access
// method In this context, the None variant represents that the element does not
// expire
pub struct AccessDuration {
	// Duration after which the grants generated with the access method expire
	// For access methods whose grants are tokens, this value is irrelevant
	pub grant: Option<Expr>,
	// Duration after which the tokens obtained with the access method expire
	// For access methods that cannot issue tokens, this value is irrelevant
	pub token: Option<Expr>,
	// Duration after which the session authenticated with the access method expires
	pub session: Option<Expr>,
}

impl Default for AccessDuration {
	fn default() -> Self {
		Self {
			// By default, access grants expire in 30 days.
			grant: Some(Expr::Literal(Literal::Duration(
				PublicDuration::from_days(30).expect("30 days should fit in a duration"),
			))),
			// By default, tokens expire after one hour
			token: Some(Expr::Literal(Literal::Duration(
				PublicDuration::from_hours(1).expect("1 hour should fit in a duration"),
			))),
			// By default, sessions do not expire
			session: None,
		}
	}
}

impl From<AccessDuration> for crate::expr::access::AccessDuration {
	fn from(v: AccessDuration) -> Self {
		Self {
			grant: v.grant.map(Into::into),
			token: v.token.map(Into::into),
			session: v.session.map(Into::into),
		}
	}
}

impl From<crate::expr::access::AccessDuration> for AccessDuration {
	fn from(v: crate::expr::access::AccessDuration) -> Self {
		Self {
			grant: v.grant.map(Into::into),
			token: v.token.map(Into::into),
			session: v.session.map(Into::into),
		}
	}
}
