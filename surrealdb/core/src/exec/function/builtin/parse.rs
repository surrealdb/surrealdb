//! Parse functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// Email parsing
define_pure_function!(ParseEmailHost, "parse::email::host", effects: EffectSet::PURE, (email: String) -> String, crate::fnc::parse::email::host);
define_pure_function!(ParseEmailUser, "parse::email::user", effects: EffectSet::PURE, (email: String) -> String, crate::fnc::parse::email::user);

// URL parsing
define_pure_function!(ParseUrlDomain, "parse::url::domain", effects: EffectSet::PURE, (url: String) -> String, crate::fnc::parse::url::domain);
define_pure_function!(ParseUrlFragment, "parse::url::fragment", effects: EffectSet::PURE, (url: String) -> String, crate::fnc::parse::url::fragment);
define_pure_function!(ParseUrlHost, "parse::url::host", effects: EffectSet::PURE, (url: String) -> String, crate::fnc::parse::url::host);
define_pure_function!(ParseUrlPath, "parse::url::path", effects: EffectSet::PURE, (url: String) -> String, crate::fnc::parse::url::path);
define_pure_function!(ParseUrlPort, "parse::url::port", effects: EffectSet::PURE, (url: String) -> Int, crate::fnc::parse::url::port);
define_pure_function!(ParseUrlQuery, "parse::url::query", effects: EffectSet::PURE, (url: String) -> String, crate::fnc::parse::url::query);
define_pure_function!(ParseUrlScheme, "parse::url::scheme", effects: EffectSet::PURE, (url: String) -> String, crate::fnc::parse::url::scheme);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		ParseEmailHost,
		ParseEmailUser,
		ParseUrlDomain,
		ParseUrlFragment,
		ParseUrlHost,
		ParseUrlPath,
		ParseUrlPort,
		ParseUrlQuery,
		ParseUrlScheme,
	);
}
