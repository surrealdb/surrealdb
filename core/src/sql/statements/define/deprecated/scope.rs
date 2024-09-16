use crate::sql::{
	access::AccessDuration,
	access_type::{JwtAccessIssue, JwtAccessVerify, JwtAccessVerifyKey},
	statements::DefineAccessStatement,
	AccessType, Algorithm, Base, Duration, Ident, JwtAccess, RecordAccess, Strand, Value,
};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineScopeStatement {
	pub name: Ident,
	pub code: String,
	pub session: Option<Duration>,
	pub signup: Option<Value>,
	pub signin: Option<Value>,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl From<DefineScopeStatement> for DefineAccessStatement {
	fn from(sc: DefineScopeStatement) -> DefineAccessStatement {
		DefineAccessStatement {
			name: sc.name,
			base: Base::Db,
			comment: sc.comment,
			if_not_exists: sc.if_not_exists,
			kind: AccessType::Record(RecordAccess {
				signup: sc.signup,
				signin: sc.signin,
				jwt: JwtAccess {
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs512,
						key: sc.code.clone(),
					}),
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs512,
						key: sc.code,
					}),
				},
			}),
			// unused fields
			authenticate: None,
			duration: AccessDuration {
				session: sc.session,
				..AccessDuration::default()
			},
			overwrite: false,
		}
	}
}
