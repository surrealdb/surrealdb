//! Lowering of a stored analyzer function name into an [`expr::Function`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below core,
//! so it cannot name `expr` types.

/// Converts a stored analyzer function reference into an executable function.
pub(crate) fn function_from_storage(name: &str) -> crate::expr::Function {
	if let Some(rest) = name.strip_prefix("mod::") {
		let mut parts = rest.split("::");
		let module = parts.next().unwrap_or_default().to_owned();
		let sub = parts.collect::<Vec<_>>().join("::");
		let sub = if sub.is_empty() {
			None
		} else {
			Some(sub)
		};
		crate::expr::Function::Module(module, sub)
	} else {
		let name = name.strip_prefix("fn::").unwrap_or(name);
		crate::expr::Function::Custom(name.to_owned())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn function_from_storage_fn_legacy() {
		assert!(matches!(
			function_from_storage("foo::bar"),
			crate::expr::Function::Custom(s) if s == "foo::bar"
		));
	}

	#[test]
	fn function_from_storage_fn_prefix() {
		assert!(matches!(
			function_from_storage("fn::foo::bar"),
			crate::expr::Function::Custom(s) if s == "foo::bar"
		));
	}

	#[test]
	fn function_from_storage_mod() {
		assert!(matches!(
			function_from_storage("mod::demo::math::add"),
			crate::expr::Function::Module(m, Some(s)) if m == "demo" && s == "math::add"
		));
	}
}
