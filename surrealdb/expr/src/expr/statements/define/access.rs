use rand::distr::{Alphanumeric, SampleString};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::access::AccessDuration;
use crate::expr::access_type::JwtAccessVerify;
use crate::expr::{AccessType, Base, CoverStmts, Expr, JwtAccess, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub context: Option<Expr>,
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
			context: None,
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
		write_sql!(f, fmt, "DEFINE ACCESS");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => {
				write_sql!(f, fmt, " OVERWRITE");
			}
			DefineKind::IfNotExists => {
				write_sql!(f, fmt, " IF NOT EXISTS");
			}
		}
		// The specific access method definition is displayed by AccessType
		write_sql!(
			f,
			fmt,
			" {} ON {} TYPE {}",
			CoverStmts(&self.name),
			self.base,
			self.access_type
		);
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write_sql!(f, fmt, " AUTHENTICATE {}", CoverStmts(v))
		}
		// The authorisation context clause. Printed before DURATION so a
		// rendered definition re-parses to the same statement.
		if let Some(ref v) = self.context {
			write_sql!(f, fmt, " CONTEXT {}", CoverStmts(v))
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write_sql!(f, fmt, " DURATION");
		if self.access_type.can_issue_grants() {
			write_sql!(f, fmt, " FOR GRANT {},", CoverStmts(&self.duration.grant));
		}
		if self.access_type.can_issue_tokens() {
			write_sql!(f, fmt, " FOR TOKEN {},", CoverStmts(&self.duration.token));
		}

		write_sql!(f, fmt, " FOR SESSION {}", CoverStmts(&self.duration.session));
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}
