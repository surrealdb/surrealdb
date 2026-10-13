//! Parse functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

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

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, ParseEmailHost, ParseEmailUser);
	#[cfg(not(feature = "url"))]
	super::disabled::register(
		registry,
		"url",
		&[
			"parse::url::domain",
			"parse::url::fragment",
			"parse::url::host",
			"parse::url::path",
			"parse::url::port",
			"parse::url::query",
			"parse::url::scheme",
		],
	);
	#[cfg(feature = "url")]
	register_functions!(
		registry,
		ParseUrlDomain,
		ParseUrlFragment,
		ParseUrlHost,
		ParseUrlPath,
		ParseUrlPort,
		ParseUrlQuery,
		ParseUrlScheme,
	);
}
