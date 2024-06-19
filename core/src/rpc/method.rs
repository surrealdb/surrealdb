#[cfg(test)]
use strum::IntoEnumIterator;
#[cfg(test)]
use strum_macros::EnumIter;

#[non_exhaustive]
#[cfg_attr(test, derive(Debug, Copy, Clone, PartialEq, EnumIter))]
pub enum Method {
	Unknown,
	Ping,
	Info,
	Use,
	Signup,
	Signin,
	Invalidate,
	Authenticate,
	Kill,
	Live,
	Set,
	Unset,
	Select,
	Insert,
	Create,
	Update,
	Merge,
	Patch,
	Delete,
	Version,
	Query,
	Relate,
	Run,
}

impl Method {
	pub fn parse<S>(s: S) -> Self
	where
		S: AsRef<str>,
	{
		match s.as_ref().to_lowercase().as_str() {
			"ping" => Self::Ping,
			"info" => Self::Info,
			"use" => Self::Use,
			"signup" => Self::Signup,
			"signin" => Self::Signin,
			"invalidate" => Self::Invalidate,
			"authenticate" => Self::Authenticate,
			"kill" => Self::Kill,
			"live" => Self::Live,
			"let" | "set" => Self::Set,
			"unset" => Self::Unset,
			"select" => Self::Select,
			"insert" => Self::Insert,
			"create" => Self::Create,
			"update" => Self::Update,
			"merge" => Self::Merge,
			"patch" => Self::Patch,
			"delete" => Self::Delete,
			"version" => Self::Version,
			"query" => Self::Query,
			"relate" => Self::Relate,
			"run" => Self::Run,
			_ => Self::Unknown,
		}
	}
}

impl Method {
	pub fn to_str(&self) -> &str {
		match self {
			Self::Unknown => "unknown",
			Self::Ping => "ping",
			Self::Info => "info",
			Self::Use => "use",
			Self::Signup => "signup",
			Self::Signin => "signin",
			Self::Invalidate => "invalidate",
			Self::Authenticate => "authenticate",
			Self::Kill => "kill",
			Self::Live => "live",
			Self::Set => "set",
			Self::Unset => "unset",
			Self::Select => "select",
			Self::Insert => "insert",
			Self::Create => "create",
			Self::Update => "update",
			Self::Merge => "merge",
			Self::Patch => "patch",
			Self::Delete => "delete",
			Self::Version => "version",
			Self::Query => "query",
			Self::Relate => "relate",
			Self::Run => "run",
		}
	}
}

impl From<u8> for Method {
	fn from(n: u8) -> Self {
		match n {
			1 => Self::Ping,
			2 => Self::Info,
			3 => Self::Use,
			4 => Self::Signup,
			5 => Self::Signin,
			6 => Self::Invalidate,
			7 => Self::Authenticate,
			8 => Self::Kill,
			9 => Self::Live,
			10 => Self::Set,
			11 => Self::Unset,
			12 => Self::Select,
			13 => Self::Insert,
			14 => Self::Create,
			15 => Self::Update,
			16 => Self::Merge,
			17 => Self::Patch,
			18 => Self::Delete,
			19 => Self::Version,
			20 => Self::Query,
			21 => Self::Relate,
			22 => Self::Run,
			_ => Self::Unknown,
		}
	}
}

impl Method {
	pub fn is_valid(&self) -> bool {
		!matches!(self, Self::Unknown)
	}

	pub fn needs_mut(&self) -> bool {
		!self.can_be_immut()
	}

	// should be the same as execute_immut
	pub fn can_be_immut(&self) -> bool {
		matches!(
			self,
			Method::Ping
				| Method::Info | Method::Select
				| Method::Insert | Method::Create
				| Method::Update | Method::Merge
				| Method::Patch | Method::Delete
				| Method::Version
				| Method::Query | Method::Relate
				| Method::Run | Method::Unknown
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn all_variants_from_u8() {
		for method in Method::iter() {
			assert_eq!(method.clone(), Method::from(method as u8));
		}
	}

	#[test]
	fn unknown_from_out_of_range_u8() {
		assert_eq!(Method::Unknown, Method::from(182));
	}
}