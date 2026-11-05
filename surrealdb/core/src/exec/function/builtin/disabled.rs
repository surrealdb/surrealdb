//! Stand-ins for a function family a cargo feature left out.
//!
//! Each keeps its name resolving, accepts any arguments, and fails every call
//! with an error that names the function and the feature the build lacks.

use anyhow::Result;

use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::val::Value;

#[derive(Debug, Clone, Copy)]
pub(super) struct Disabled {
	pub(super) name: &'static str,
	pub(super) feature: &'static str,
}

impl ScalarFunction for Disabled {
	fn name(&self) -> &'static str {
		self.name
	}

	fn signature(&self) -> Signature {
		Signature::new().variadic(Kind::Any).returns(Kind::Any)
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY
	}

	fn fallibility(&self) -> Fallibility {
		Fallibility::VALUE | Fallibility::CONTEXT | Fallibility::DATA
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::Error::new(crate::exec::Error::InvalidFunction {
			name: self.name.to_owned(),
			message: format!(
				"This build was compiled without the `{}` feature, which this function requires.",
				self.feature
			),
		}))
	}
}

/// The stand-ins for the `api` builtins, in a build without the `api` feature.
#[cfg(not(feature = "api"))]
pub(super) const API: &[&dyn ScalarFunction] = &[
	&Disabled {
		name: "api::invoke",
		feature: "api",
	},
	&Disabled {
		name: "api::req::body",
		feature: "api",
	},
	&Disabled {
		name: "api::req::max_body",
		feature: "api",
	},
	&Disabled {
		name: "api::res::body",
		feature: "api",
	},
	&Disabled {
		name: "api::res::status",
		feature: "api",
	},
	&Disabled {
		name: "api::res::header",
		feature: "api",
	},
	&Disabled {
		name: "api::res::headers",
		feature: "api",
	},
	&Disabled {
		name: "api::timeout",
		feature: "api",
	},
];

/// The stand-ins for the `encoding` builtins, in a build without the `encoding` feature.
#[cfg(not(feature = "encoding"))]
pub(super) const ENCODING: &[&dyn ScalarFunction] = &[
	&Disabled {
		name: "encoding::base64::decode",
		feature: "encoding",
	},
	&Disabled {
		name: "encoding::base64::encode",
		feature: "encoding",
	},
	&Disabled {
		name: "encoding::cbor::decode",
		feature: "encoding",
	},
	&Disabled {
		name: "encoding::cbor::encode",
		feature: "encoding",
	},
	&Disabled {
		name: "encoding::json::decode",
		feature: "encoding",
	},
	&Disabled {
		name: "encoding::json::encode",
		feature: "encoding",
	},
];

/// The stand-ins for the `files` builtins, in a build without the `files` feature.
#[cfg(not(feature = "files"))]
pub(super) const FILES: &[&dyn ScalarFunction] = &[
	&Disabled {
		name: "file::put",
		feature: "files",
	},
	&Disabled {
		name: "file::put_if_not_exists",
		feature: "files",
	},
	&Disabled {
		name: "file::get",
		feature: "files",
	},
	&Disabled {
		name: "file::head",
		feature: "files",
	},
	&Disabled {
		name: "file::delete",
		feature: "files",
	},
	&Disabled {
		name: "file::copy",
		feature: "files",
	},
	&Disabled {
		name: "file::copy_if_not_exists",
		feature: "files",
	},
	&Disabled {
		name: "file::rename",
		feature: "files",
	},
	&Disabled {
		name: "file::rename_if_not_exists",
		feature: "files",
	},
	&Disabled {
		name: "file::exists",
		feature: "files",
	},
	&Disabled {
		name: "file::list",
		feature: "files",
	},
	&Disabled {
		name: "file::presign_get",
		feature: "files",
	},
	&Disabled {
		name: "file::presign_put",
		feature: "files",
	},
	&Disabled {
		name: "file::bucket",
		feature: "files",
	},
	&Disabled {
		name: "file::key",
		feature: "files",
	},
];

/// The stand-ins for the `search` builtins, in a build without the `search` feature.
#[cfg(not(feature = "search"))]
pub(super) const SEARCH: &[&dyn ScalarFunction] = &[
	&Disabled {
		name: "search::analyze",
		feature: "search",
	},
	&Disabled {
		name: "search::rrf",
		feature: "search",
	},
	&Disabled {
		name: "search::linear",
		feature: "search",
	},
	&Disabled {
		name: "search::highlight",
		feature: "search",
	},
	&Disabled {
		name: "search::score",
		feature: "search",
	},
	&Disabled {
		name: "search::offsets",
		feature: "search",
	},
];
