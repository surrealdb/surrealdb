//! `sql` -> `expr` conversions for [`crate::sql::statements::access`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::access::*;

impl From<AccessStatement> for crate::expr::statements::access::AccessStatement {
	fn from(v: AccessStatement) -> Self {
		match v {
			AccessStatement::Grant(v) => Self::Grant(v.into()),
			AccessStatement::Show(v) => Self::Show(v.into()),
			AccessStatement::Revoke(v) => Self::Revoke(v.into()),
			AccessStatement::Purge(v) => Self::Purge(v.into()),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatement> for AccessStatement {
	fn from(v: crate::expr::statements::access::AccessStatement) -> Self {
		match v {
			crate::expr::statements::access::AccessStatement::Grant(v) => Self::Grant(v.into()),
			crate::expr::statements::access::AccessStatement::Show(v) => Self::Show(v.into()),
			crate::expr::statements::access::AccessStatement::Revoke(v) => Self::Revoke(v.into()),
			crate::expr::statements::access::AccessStatement::Purge(v) => Self::Purge(v.into()),
		}
	}
}

impl From<AccessStatementGrant> for crate::expr::statements::access::AccessStatementGrant {
	fn from(v: AccessStatementGrant) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			subject: v.subject.into(),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementGrant> for AccessStatementGrant {
	fn from(v: crate::expr::statements::access::AccessStatementGrant) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			subject: v.subject.into(),
		}
	}
}

impl From<AccessStatementShow> for crate::expr::statements::access::AccessStatementShow {
	fn from(v: AccessStatementShow) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementShow> for AccessStatementShow {
	fn from(v: crate::expr::statements::access::AccessStatementShow) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<AccessStatementRevoke> for crate::expr::statements::access::AccessStatementRevoke {
	fn from(v: AccessStatementRevoke) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementRevoke> for AccessStatementRevoke {
	fn from(v: crate::expr::statements::access::AccessStatementRevoke) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<AccessStatementPurge> for crate::expr::statements::access::AccessStatementPurge {
	fn from(v: AccessStatementPurge) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(From::from),
			kind: v.kind.into(),
			grace: v.grace.into(),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementPurge> for AccessStatementPurge {
	fn from(v: crate::expr::statements::access::AccessStatementPurge) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(From::from),
			kind: v.kind.into(),
			grace: v.grace.into(),
		}
	}
}

impl From<PurgeKind> for crate::expr::statements::access::PurgeKind {
	fn from(v: PurgeKind) -> Self {
		match v {
			PurgeKind::Expired => crate::expr::statements::access::PurgeKind::Expired,
			PurgeKind::Revoked => crate::expr::statements::access::PurgeKind::Revoked,
			PurgeKind::Both => crate::expr::statements::access::PurgeKind::Both,
		}
	}
}

impl From<crate::expr::statements::access::PurgeKind> for PurgeKind {
	fn from(v: crate::expr::statements::access::PurgeKind) -> Self {
		match v {
			crate::expr::statements::access::PurgeKind::Expired => PurgeKind::Expired,
			crate::expr::statements::access::PurgeKind::Revoked => PurgeKind::Revoked,
			crate::expr::statements::access::PurgeKind::Both => PurgeKind::Both,
		}
	}
}

impl From<Subject> for crate::expr::statements::access::Subject {
	fn from(v: Subject) -> Self {
		match v {
			Subject::Record(id) => Self::Record(id.into()),
			Subject::User(name) => Self::User(name),
		}
	}
}

impl From<crate::expr::statements::access::Subject> for Subject {
	fn from(v: crate::expr::statements::access::Subject) -> Self {
		match v {
			crate::expr::statements::access::Subject::Record(id) => Self::Record(id.into()),
			crate::expr::statements::access::Subject::User(name) => Self::User(name),
		}
	}
}
