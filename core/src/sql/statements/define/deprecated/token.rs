use crate::sql::{
	access::AccessDuration,
	access_type::{JwtAccessVerify, JwtAccessVerifyKey},
	statements::DefineAccessStatement,
	AccessType, Algorithm, Base, Ident, JwtAccess, Strand,
};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineTokenStatement {
	pub name: Ident,
	pub base: Base,
	pub kind: Algorithm,
	pub code: String,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl From<DefineTokenStatement> for DefineAccessStatement {
	fn from(tk: DefineTokenStatement) -> DefineAccessStatement {
		DefineAccessStatement {
			name: tk.name,
			base: tk.base,
			comment: tk.comment,
			if_not_exists: tk.if_not_exists,
			kind: AccessType::Jwt(JwtAccess {
				issue: None,
				verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: tk.kind,
					key: tk.code,
				}),
			}),
			// unused fields
			authenticate: None,
			duration: AccessDuration::default(),
			overwrite: false,
		}
	}
}
