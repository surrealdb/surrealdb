//! Macros for defining scalar functions with minimal boilerplate.

/// Define a pure scalar function that wraps an existing `fnc::*` implementation.
///
/// # Usage
///
/// ```ignore
/// // Simple function with one argument
/// define_pure_function!(
///     MathAbs,                        // Struct name
///     "math::abs",                    // Function name
///     (value: Number) -> Number,      // Signature: (args) -> return
///     crate::fnc::math::abs           // Implementation path
/// );
///
/// // Function with multiple arguments
/// define_pure_function!(
///     MathClamp,
///     "math::clamp",
///     (value: Number, min: Number, max: Number) -> Number,
///     crate::fnc::math::clamp
/// );
///
/// // Function with no arguments
/// define_pure_function!(
///     Rand,
///     "rand",
///     () -> Float,
///     crate::fnc::rand::rand
/// );
///
/// // Function with optional arguments
/// define_pure_function!(
///     MathRound,
///     "math::round",
///     (value: Number, ?precision: Number) -> Number,
///     crate::fnc::math::round
/// );
///
/// // Function with variadic arguments
/// define_pure_function!(
///     StringConcat,
///     "string::concat",
///     (...values: Any) -> String,
///     crate::fnc::string::concat
/// );
/// ```
#[macro_export]
macro_rules! define_pure_function {
	// No arguments: () -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		() -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// Single required argument: (name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg_name), $crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// Two required arguments: (a: Type1, b: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// Three required arguments
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident, $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.arg(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// Variadic: (...name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		(... $arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.variadic($crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// One required + variadic: (first: Type1, ...rest: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, ... $rest_name:ident : $rest_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.variadic($crate::expr::Kind::$rest_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// One required + one optional: (req: Type1, ?opt: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// Two required + one optional: (a: T1, b: T2, ?c: T3) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};

	// Two optional arguments: (?a: T1, ?b: T2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		(? $arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.optional(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$impl_path(args)
			}
		}
	};
}

/// Helper macro to register multiple functions at once.
///
/// # Usage
///
/// ```ignore
/// register_functions!(registry,
///     MathAbs,
///     MathCeil,
///     MathFloor,
///     // ...
/// );
/// ```
#[macro_export]
macro_rules! register_functions {
	($registry:expr, $($func:ty),* $(,)?) => {
		$(
			$registry.register(<$func>::default());
		)*
	};
}

/// Define a context-aware scalar function that needs access to EvalContext.
///
/// Context-aware functions are not pure (they depend on session state) but are
/// not async. They synchronously access context information like session::ns(),
/// session::db(), etc.
///
/// # Usage
///
/// ```ignore
/// // Function with no arguments that reads from context
/// define_context_function!(
///     SessionNs,                      // Struct name
///     "session::ns",                  // Function name
///     () -> Any,                      // Signature: () -> return type
///     session_ns_impl                 // Implementation function
/// );
/// ```
#[macro_export]
macro_rules! define_context_function {
	// No arguments: () -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		() -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires context", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				_args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx) })
			}
		}
	};
}

/// Define an async scalar function that needs access to EvalContext.
///
/// Async functions are used for:
/// - I/O-bound operations (HTTP requests)
/// - CPU-intensive operations (crypto hashing)
/// - Timer-based operations (sleep)
///
/// # Usage
///
/// ```ignore
/// // Async function with one argument
/// define_async_function!(
///     Sleep,                          // Struct name
///     "sleep",                        // Function name
///     (duration: Duration) -> None,   // Signature
///     sleep_impl                      // Async implementation function
/// );
///
/// // Async function with two arguments
/// define_async_function!(
///     HttpGet,
///     "http::get",
///     (url: String, ?opts: Object) -> Any,
///     http_get_impl
/// );
/// ```
#[macro_export]
macro_rules! define_async_function {
	// No arguments: () -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		() -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				_args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx).await })
			}
		}
	};

	// Single required argument: (name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg_name), $crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx, args).await })
			}
		}
	};

	// Two required arguments: (a: Type1, b: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx, args).await })
			}
		}
	};

	// One required + one optional: (req: Type1, ?opt: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx, args).await })
			}
		}
	};

	// Two required + one optional: (a: T1, b: T2, ?c: T3) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx, args).await })
			}
		}
	};

	// One required + two optional: (req: Type1, ?opt1: Type2, ?opt2: Type3) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::physical_expr::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move { $impl_fn(ctx, args).await })
			}
		}
	};
}

// Note: The macros are exported from the crate root via #[macro_export]
// so they can be used as crate::define_pure_function and crate::register_functions
