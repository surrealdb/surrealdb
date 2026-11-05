//! Parse functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

// Email parsing
define_pure_function!(ParseEmailHost, "parse::email::host", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (email: String) -> String, crate::fnc::parse::email::host);
define_pure_function!(ParseEmailUser, "parse::email::user", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (email: String) -> String, crate::fnc::parse::email::user);

// URL parsing, which needs the `url` feature.
#[cfg(feature = "url")]
define_pure_function!(ParseUrlDomain, "parse::url::domain", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> String, crate::fnc::parse::url::domain);
#[cfg(feature = "url")]
define_pure_function!(ParseUrlFragment, "parse::url::fragment", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> String, crate::fnc::parse::url::fragment);
#[cfg(feature = "url")]
define_pure_function!(ParseUrlHost, "parse::url::host", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> String, crate::fnc::parse::url::host);
#[cfg(feature = "url")]
define_pure_function!(ParseUrlPath, "parse::url::path", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> String, crate::fnc::parse::url::path);
#[cfg(feature = "url")]
define_pure_function!(ParseUrlPort, "parse::url::port", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> Int, crate::fnc::parse::url::port);
#[cfg(feature = "url")]
define_pure_function!(ParseUrlQuery, "parse::url::query", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> String, crate::fnc::parse::url::query);
#[cfg(feature = "url")]
define_pure_function!(ParseUrlScheme, "parse::url::scheme", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (url: String) -> String, crate::fnc::parse::url::scheme);

/// The `parse::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&ParseEmailHost,
	&ParseEmailUser,
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::domain",
		feature: "url",
	},
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::fragment",
		feature: "url",
	},
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::host",
		feature: "url",
	},
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::path",
		feature: "url",
	},
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::port",
		feature: "url",
	},
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::query",
		feature: "url",
	},
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "parse::url::scheme",
		feature: "url",
	},
	#[cfg(feature = "url")]
	&ParseUrlDomain,
	#[cfg(feature = "url")]
	&ParseUrlFragment,
	#[cfg(feature = "url")]
	&ParseUrlHost,
	#[cfg(feature = "url")]
	&ParseUrlPath,
	#[cfg(feature = "url")]
	&ParseUrlPort,
	#[cfg(feature = "url")]
	&ParseUrlQuery,
	#[cfg(feature = "url")]
	&ParseUrlScheme,
];
