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

impl Into<DefineAccessStatement> for DefineScopeStatement {
	fn into(self) -> DefineAccessStatement {
		DefineAccessStatement {
			name: self.name,
			base: Base::Db,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
			kind: AccessType::Record(RecordAccess {
				signup: self.signup,
				signin: self.signin,
				jwt: JwtAccess {
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs512,
						key: self.code.clone(),
					}),
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs512,
						key: self.code,
					}),
				},
			}),
			// unused fields
			authenticate: None,
			duration: AccessDuration {
				session: self.session,
				..AccessDuration::default()
			},
			overwrite: false,
		}
	}
}
