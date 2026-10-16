//! Macros for defining scalar functions with minimal boilerplate.

/// Names a builtin's fallibility class and checks it at compile time against the `FALLIBILITY`
/// table entry for `(kind, name)`.
///
/// Expands to the class, so it stands in the body of `fn fallibility`. `kind` is a `FnKind`
/// variant, `name` the builtin's registered name, and `class` a constant expression. A builtin
/// with no table entry, or one whose entry holds a different class, is a build error.
#[macro_export]
macro_rules! declare_fallibility {
	($kind:ident, $name:literal, $class:expr) => {{
		const CLASS: $crate::exec::ir::iface::Fallibility = $class;
		const _: () = $crate::exec::function::check_fallibility_row(
			$crate::exec::function::FnKind::$kind,
			$name,
			CLASS,
		);
		CLASS
	}};
}

/// The `invoke` (and, for a metered function, `metered`) items of a
/// [`define_pure_function!`] expansion, written inside the `ScalarFunction`
/// impl.
///
/// Without a walk, `invoke` decodes the arguments and calls the implementation.
/// With one, `invoke` runs the walk to its end through
/// [`run_walk`](crate::exec::function::run_walk) and `metered` hands the
/// executor the same walk; both go through `metered_walk`, which
/// `__pure_walk!` defines.
#[doc(hidden)]
#[macro_export]
macro_rules! __pure_invoke {
	($func_name:literal, $impl_path:path, []) => {
		fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
			let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
			$impl_path(args)
		}
	};
	($func_name:literal, $impl_path:path, [$metered:path]) => {
		fn invoke(&self, args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
			$crate::exec::function::run_walk(::std::pin::pin!(Self::metered_walk(args)))
		}

		fn metered(&self) -> Option<$crate::exec::function::MeteredFn> {
			Some(|args| Box::pin(Self::metered_walk(args)))
		}
	};
}

/// The `metered_walk` of a metered [`define_pure_function!`] expansion, written
/// after the `ScalarFunction` impl; nothing for a function without a walk.
#[doc(hidden)]
#[macro_export]
macro_rules! __pure_walk {
	($struct_name:ident, $func_name:literal, []) => {};
	($struct_name:ident, $func_name:literal, [$metered:path]) => {
		impl $struct_name {
			/// The builtin's walk over its arguments, as `metered` and `invoke` run it.
			async fn metered_walk(
				args: Vec<$crate::val::Value>,
			) -> anyhow::Result<$crate::val::Value> {
				let args = $crate::fnc::args::FromArgs::from_args($func_name, args)?;
				$metered(args).await
			}
		}
	};
}

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
///
/// Every function names what its failure depends on after `effects:`, as
/// `fallibility: Fallibility::VALUE,`
/// ([`ScalarFunction::fallibility`](crate::exec::function::ScalarFunction::fallibility)).
/// The argument is required: a function without it does not compile.
///
/// A function whose work grows with its input also names, after any `rule:`,
/// the walk the executor runs it as (`metered: crate::fnc::array::sort_walk,`):
/// an `async fn` over the same arguments that charges the fuel meter a
/// stretch at a time
/// ([`ScalarFunction::metered`](crate::exec::function::ScalarFunction::metered)).
/// For such a function `invoke` runs the walk to its end
/// ([`run_walk`](crate::exec::function::run_walk)), and the implementation path
/// names the synchronous reference that `fnc::synchronous` dispatches to.
#[macro_export]
macro_rules! define_pure_function {
	// No arguments: () -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		() -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Single required argument: (name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg_name), $crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Two required arguments: (a: Type1, b: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Three required arguments
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident, $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.arg(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Variadic: (...name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		(... $arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.variadic($crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// One required + variadic: (first: Type1, ...rest: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, ... $rest_name:ident : $rest_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.variadic($crate::expr::Kind::$rest_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Single optional argument: (?name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		(? $arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.optional(stringify!($arg_name), $crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// One required + one optional: (req: Type1, ?opt: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Two required + one optional: (a: T1, b: T2, ?c: T3) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// One required + two optional: (a: T1, ?b: T2, ?c: T3) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Two required + two optional: (a: T1, b: T2, ?c: T3, ?d: T4) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident, ? $arg4_name:ident : $arg4_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.optional(stringify!($arg4_name), $crate::expr::Kind::$arg4_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
	};

	// Two optional arguments: (?a: T1, ?b: T2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		$(metered: $metered:path,)?
		(? $arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_path:path
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.optional(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			$crate::__pure_invoke!($func_name, $impl_path, [$($metered)?]);
		}

		$crate::__pure_walk!($struct_name, $func_name, [$($metered)?]);
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
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		() -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn invoke(&self, _args: Vec<$crate::val::Value>) -> anyhow::Result<$crate::val::Value> {
				Err(anyhow::anyhow!("Function '{}' requires context", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a $crate::exec::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				let result = match $crate::exec::function::check_arity($func_name, &args, 0, Some(0)) {
					Ok(()) => $impl_fn(ctx),
					Err(e) => Err(e),
				};
				$crate::exec::function::ready_boxed(result)
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
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		() -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
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
				ctx: &'a $crate::exec::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move {
					$crate::exec::function::check_arity($func_name, &args, 0, Some(0))?;
					$impl_fn(ctx).await
				})
			}
		}
	};

	// Single required argument: (name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		($arg_name:ident : $arg_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg_name), $crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
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
				ctx: &'a $crate::exec::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move {
					$crate::exec::function::check_arity($func_name, &args, 1, Some(1))?;
					$impl_fn(ctx, args).await
				})
			}
		}
	};

	// Two required arguments: (a: Type1, b: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		($arg1_name:ident : $arg1_type:ident, $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.arg(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
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
				ctx: &'a $crate::exec::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move {
					$crate::exec::function::check_arity($func_name, &args, 2, Some(2))?;
					$impl_fn(ctx, args).await
				})
			}
		}
	};

	// One required + one optional: (req: Type1, ?opt: Type2) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
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
				ctx: &'a $crate::exec::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move {
					$crate::exec::function::check_arity($func_name, &args, 1, Some(2))?;
					$impl_fn(ctx, args).await
				})
			}
		}
	};

	// One required + two optional: (req: Type1, ?opt1: Type2, ?opt2: Type3) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		effects: $effects:expr,
		fallibility: $fallibility:expr,
		$(rule: $rule:expr,)?
		($arg1_name:ident : $arg1_type:ident, ? $arg2_name:ident : $arg2_type:ident, ? $arg3_name:ident : $arg3_type:ident) -> $ret:ident,
		$impl_fn:expr
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> $crate::expr::logical::EffectSet {
				#[allow(unused_imports)]
				use $crate::expr::logical::EffectSet;
				$effects
			}

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				#[allow(unused_imports)]
				use $crate::exec::ir::iface::Fallibility;
				$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg1_name), $crate::expr::Kind::$arg1_type)
					.optional(stringify!($arg2_name), $crate::expr::Kind::$arg2_type)
					.optional(stringify!($arg3_name), $crate::expr::Kind::$arg3_type)
					.returns($crate::expr::Kind::$ret)
					$(.rule({
						#[allow(unused_imports)]
						use $crate::exec::function::ReturnRule;
						$rule
					}))?
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
				ctx: &'a $crate::exec::EvalContext<'_>,
				args: Vec<$crate::val::Value>,
			) -> $crate::exec::BoxFut<'a, anyhow::Result<$crate::val::Value>> {
				Box::pin(async move {
					$crate::exec::function::check_arity($func_name, &args, 1, Some(3))?;
					$impl_fn(ctx, args).await
				})
			}
		}
	};
}

// Note: The macros are exported from the crate root via #[macro_export]
// so they can be used as crate::define_pure_function and crate::define_async_function
