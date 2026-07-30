use rand::distr::{Alphanumeric, SampleString};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::expr::access::AccessDuration;
use crate::expr::access_type::JwtAccessVerify;
use crate::expr::{AccessType, Base, Expr, JwtAccess, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Expr,
}

impl Default for DefineAccessStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			access_type: AccessType::default(),
			authenticate: None,
			duration: AccessDuration::default(),
			comment: Expr::Literal(Literal::None),
		}
	}
}

impl DefineAccessStatement {
	/// Generate a random key to be used to sign session tokens
	/// This key will be used to sign tokens issued with this access method
	/// This value is used by default in every access method other than JWT
	pub fn random_key() -> String {
		Alphanumeric.sample_string(&mut rand::rng(), 128)
	}
}

impl DefineAccessStatement {
	/// Remove information from the access definition which should not be displayed.
	pub fn redact(mut self) -> Self {
		fn redact_jwt_access(acc: &mut JwtAccess) {
			if let JwtAccessVerify::Key(ref mut v) = acc.verify
				&& v.alg.is_symmetric()
			{
				v.key = Expr::Literal(Literal::String(Strand::new_static("[REDACTED]")));
			}
			if let Some(ref mut s) = acc.issue {
				s.key = Expr::Literal(Literal::String(Strand::new_static("[REDACTED]")));
			}
		}

		match self.access_type {
			AccessType::Jwt(ref mut key) => {
				redact_jwt_access(key);
			}
			AccessType::Bearer(ref mut b) => {
				redact_jwt_access(&mut b.jwt);
			}
			AccessType::Record(ref mut r) => {
				redact_jwt_access(&mut r.jwt);
				if let Some(ref mut b) = r.bearer {
					redact_jwt_access(&mut b.jwt);
				}
			}
		}
		self
	}
}

impl ToSql for DefineAccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::DefineAccessStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
