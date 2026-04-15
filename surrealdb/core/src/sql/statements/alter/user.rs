use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, SaltString};
use rand::rngs::OsRng;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{EscapeKwFreeIdent, QuoteStr};
use crate::sql::Base;
use crate::sql::statements::define::user::PassType;
use crate::types::PublicDuration;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER USER`.
pub struct AlterUserStatement {
	pub name: String,
	pub base: Base,
	pub if_exists: bool,
	pub pass_type: Option<PassType>,
	pub roles: AlterKind<Vec<String>>,
	pub token_duration: AlterKind<PublicDuration>,
	pub session_duration: AlterKind<PublicDuration>,
	pub comment: AlterKind<String>,
}

impl ToSql for AlterUserStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER USER");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {} ON {}", EscapeKwFreeIdent(&self.name), &self.base);

		if let Some(ref pt) = self.pass_type {
			match pt {
				PassType::Password(p) => write_sql!(f, fmt, " PASSWORD {}", QuoteStr(p)),
				PassType::Hash(h) => write_sql!(f, fmt, " PASSHASH {}", QuoteStr(h)),
				PassType::Unset => {}
			}
		}

		if let AlterKind::Set(ref roles) = self.roles {
			write_sql!(f, fmt, " ROLES");
			for (i, r) in roles.iter().enumerate() {
				if i > 0 {
					f.push(',');
				}
				write_sql!(f, fmt, " {}", EscapeKwFreeIdent(r));
			}
		}

		match self.token_duration {
			AlterKind::Set(ref d) => write_sql!(f, fmt, " DURATION FOR TOKEN {d}"),
			AlterKind::Drop => f.push_str(" DURATION FOR TOKEN NONE"),
			AlterKind::None => {}
		}

		match self.session_duration {
			AlterKind::Set(ref d) => write_sql!(f, fmt, " DURATION FOR SESSION {d}"),
			AlterKind::Drop => f.push_str(" DURATION FOR SESSION NONE"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}

impl From<AlterUserStatement> for crate::expr::statements::alter::AlterUserStatement {
	fn from(v: AlterUserStatement) -> Self {
		let hash = v.pass_type.and_then(|pt| match pt {
			PassType::Unset => None,
			PassType::Hash(h) => Some(h),
			PassType::Password(p) => Some(
				Argon2::default()
					.hash_password(p.as_bytes(), &SaltString::generate(&mut OsRng))
					.expect("password hashing should not fail")
					.to_string(),
			),
		});

		crate::expr::statements::alter::AlterUserStatement {
			name: v.name,
			base: v.base.into(),
			if_exists: v.if_exists,
			hash,
			roles: match v.roles {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(x),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			token_duration: match v.token_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			session_duration: match v.session_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterUserStatement> for AlterUserStatement {
	fn from(v: crate::expr::statements::alter::AlterUserStatement) -> Self {
		AlterUserStatement {
			name: v.name,
			base: v.base.into(),
			if_exists: v.if_exists,
			pass_type: v.hash.map(PassType::Hash),
			roles: match v.roles {
				crate::expr::statements::alter::AlterKind::Set(x) => AlterKind::Set(x),
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			token_duration: match v.token_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			session_duration: match v.session_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}
