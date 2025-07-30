use std::time::Duration;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::catalog::scope::Scope;



#[revisioned(revision = 3)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessDefinition {
	pub name: String,
	pub scope: Scope,
    // TODO: STU Implement these fields
	// pub kind: AccessType,
	// pub authenticate: Option<Value>,
	pub duration: AccessDuration,
	pub comment: Option<String>,
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// Durations representing the expiration of different elements of the access method
// In this context, the None variant represents that the element does not expire
pub struct AccessDuration {
	// Duration after which the grants generated with the access method expire
	// For access methods whose grants are tokens, this value is irrelevant
	pub grant: Option<Duration>,
	// Duration after which the tokens obtained with the access method expire
	// For access methods that cannot issue tokens, this value is irrelevant
	pub token: Option<Duration>,
	// Duration after which the session authenticated with the access method expires
	pub session: Option<Duration>,
}

// #[revisioned(revision = 2)]
// #[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
// #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// #[non_exhaustive]
// pub enum AccessType {
// 	Record(RecordAccess),
// 	Jwt(JwtAccess),
// 	// TODO(gguillemas): Document once bearer access is no longer experimental.
// 	#[revision(start = 2)]
// 	Bearer(BearerAccess),
// }

// #[revisioned(revision = 4)]
// #[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
// #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// pub struct RecordAccess {
// 	pub signup: Option<Value>,
// 	pub signin: Option<Value>,
// 	pub jwt: JwtAccess,
// 	#[revision(start = 2, end = 3, convert_fn = "authenticate_revision")]
// 	pub authenticate: Option<Value>,
// 	#[revision(start = 4)]
// 	pub bearer: Option<BearerAccess>,
// }

// #[revisioned(revision = 1)]
// #[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
// #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// pub struct JwtAccess {
// 	// Verify is required
// 	pub verify: JwtAccessVerify,
// 	// Issue is optional
// 	// It is possible to only verify externally issued tokens
// 	pub issue: Option<JwtAccessIssue>,
// }

