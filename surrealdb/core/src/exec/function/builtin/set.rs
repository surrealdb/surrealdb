//! Set functions

use anyhow::Result;

use crate::exec::EvalContext;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::exec::vm::calls::StreamingClosureEvaluator;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::{ClosureEvaluator, Value};
use crate::{define_pure_function, register_functions};

// Single set argument functions
define_pure_function!(SetFirst, "set::first", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), (set: Any) -> Any, crate::fnc::set::first);
define_pure_function!(SetFlatten, "set::flatten", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::flatten_walk, (set: Any) -> Any, crate::fnc::set::flatten);
define_pure_function!(SetIsEmpty, "set::is_empty", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (set: Any) -> Bool, crate::fnc::set::is_empty);
define_pure_function!(SetLast, "set::last", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), (set: Any) -> Any, crate::fnc::set::last);
define_pure_function!(SetLen, "set::len", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (set: Any) -> Int, crate::fnc::set::len);
define_pure_function!(SetMax, "set::max", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), metered: crate::fnc::set::max_walk, (set: Any) -> Any, crate::fnc::set::max);
define_pure_function!(SetMin, "set::min", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), metered: crate::fnc::set::min_walk, (set: Any) -> Any, crate::fnc::set::min);

// Two argument set functions
define_pure_function!(SetAdd, "set::add", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::add_walk, (set: Any, value: Any) -> Any, crate::fnc::set::add);
define_pure_function!(SetAt, "set::at", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), metered: crate::fnc::set::at_walk, (set: Any, index: Int) -> Any, crate::fnc::set::at);
define_pure_function!(SetComplement, "set::complement", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::complement_walk, (a: Any, b: Any) -> Any, crate::fnc::set::complement);
define_pure_function!(SetContains, "set::contains", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (set: Any, value: Any) -> Bool, crate::fnc::set::contains);
define_pure_function!(SetDifference, "set::difference", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::difference_walk, (a: Any, b: Any) -> Any, crate::fnc::set::difference);
define_pure_function!(SetIntersect, "set::intersect", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::intersect_walk, (a: Any, b: Any) -> Any, crate::fnc::set::intersect);
define_pure_function!(SetJoin, "set::join", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::join_walk, (set: Any, separator: String) -> String, crate::fnc::set::join);
define_pure_function!(SetRemove, "set::remove", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::remove_walk, (set: Any, value: Any) -> Any, crate::fnc::set::remove);
define_pure_function!(SetUnion, "set::union", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::union_walk, (a: Any, b: Any) -> Any, crate::fnc::set::union);

// Three argument set functions
define_pure_function!(SetSlice, "set::slice", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::set::slice_walk, (set: Any, ?start: Any, ?end: Int) -> Any, crate::fnc::set::slice);

// =========================================================================
// Closure-based set functions (require async execution)
// =========================================================================

/// A builtin that runs the closure it is handed over the elements of
/// a set. The closure is the final argument, written `|name: Kind|`, or
/// `?|name: Kind|` when it may be omitted; the macro declares its position
/// through [`ScalarFunction::closure_args`], which is what tells the effect
/// fold that a closure arriving there as a value runs a body the call cannot
/// see. A body written in the call is judged as a child of the call, and the
/// set argument never carries a body, so the declared effects are
/// those of the builtin alone: `PURE`. A trailing `, fallibility: <class>` declares
/// [`ScalarFunction::fallibility`].
macro_rules! define_set_closure_function {
	($struct_name:ident, $func_name:literal, $impl_path:path, $($arg:ident: $kind:ident,)+ ?|$carg:ident: $ckind:ident| => $ret:ident $(, fallibility: $fallibility:expr)?) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> EffectSet {
				EffectSet::PURE
			}

			$(fn fallibility(&self) -> crate::exec::ir::iface::Fallibility {
				crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			})?

			fn closure_args(&self) -> &'static [usize] {
				const SLOTS: [usize; 1] = [[$(stringify!($arg)),+].len()];
				&SLOTS
			}

			fn signature(&self) -> Signature {
				Signature::new()
					$(.arg(stringify!($arg), Kind::$kind))+
					.optional(stringify!($carg), Kind::$ckind)
					.returns(Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a EvalContext<'_>,
				args: Vec<Value>,
			) -> crate::exec::BoxFut<'a, Result<Value>> {
				Box::pin(async move {
					let args = FromArgs::from_args($func_name, args)?;
					let evaluator = StreamingClosureEvaluator::new(ctx);
					let ev = Some(&evaluator as &dyn ClosureEvaluator);
					$impl_path(ev, args)
						.await
				})
			}
		}
	};
	($struct_name:ident, $func_name:literal, $impl_path:path, $($arg:ident: $kind:ident,)+ |$carg:ident: $ckind:ident| => $ret:ident $(, fallibility: $fallibility:expr)?) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> EffectSet {
				EffectSet::PURE
			}

			$(fn fallibility(&self) -> crate::exec::ir::iface::Fallibility {
				crate::declare_fallibility!(Scalar, $func_name, $fallibility)
			})?

			fn closure_args(&self) -> &'static [usize] {
				const SLOTS: [usize; 1] = [[$(stringify!($arg)),+].len()];
				&SLOTS
			}

			fn signature(&self) -> Signature {
				Signature::new()
					$(.arg(stringify!($arg), Kind::$kind))+
					.arg(stringify!($carg), Kind::$ckind)
					.returns(Kind::$ret)
			}

			fn is_pure(&self) -> bool {
				false
			}

			fn is_async(&self) -> bool {
				true
			}

			fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
			}

			fn invoke_async<'a>(
				&'a self,
				ctx: &'a EvalContext<'_>,
				args: Vec<Value>,
			) -> crate::exec::BoxFut<'a, Result<Value>> {
				Box::pin(async move {
					let args = FromArgs::from_args($func_name, args)?;
					let evaluator = StreamingClosureEvaluator::new(ctx);
					let ev = Some(&evaluator as &dyn ClosureEvaluator);
					$impl_path(ev, args)
						.await
				})
			}
		}
	};
}

// set::all - Check if all elements match a condition
define_set_closure_function!(SetAll, "set::all", crate::fnc::set::all, set: Any, ?|check: Any| => Any, fallibility: Fallibility::VALUE);

// set::any - Check if any element matches a condition
define_set_closure_function!(SetAny, "set::any", crate::fnc::set::any, set: Any, ?|check: Any| => Any, fallibility: Fallibility::VALUE);

// set::filter - Filter elements by closure/value
define_set_closure_function!(SetFilter, "set::filter", crate::fnc::set::filter, set: Any, |check: Any| => Any, fallibility: Fallibility::VALUE);

// set::find - Find first matching element
define_set_closure_function!(SetFind, "set::find", crate::fnc::set::find, set: Any, |check: Any| => Any, fallibility: Fallibility::VALUE);

// set::fold - Fold with accumulator and closure
define_set_closure_function!(SetFold, "set::fold", crate::fnc::set::fold, set: Any, init: Any, |mapper: Any| => Any, fallibility: Fallibility::VALUE);

// set::map - Transform elements with closure
define_set_closure_function!(SetMap, "set::map", crate::fnc::set::map, set: Any, |mapper: Any| => Any, fallibility: Fallibility::VALUE);

// set::reduce - Reduce set with closure
define_set_closure_function!(SetReduce, "set::reduce", crate::fnc::set::reduce, set: Any, |mapper: Any| => Any, fallibility: Fallibility::VALUE);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		SetAdd,
		SetAt,
		SetComplement,
		SetContains,
		SetDifference,
		SetFirst,
		SetFlatten,
		SetIntersect,
		SetIsEmpty,
		SetJoin,
		SetLast,
		SetLen,
		SetMax,
		SetMin,
		SetRemove,
		SetSlice,
		SetUnion,
	);

	// Register closure-based functions
	registry.register(SetAll);
	registry.register(SetAny);
	registry.register(SetFilter);
	registry.register(SetFind);
	registry.register(SetFold);
	registry.register(SetMap);
	registry.register(SetReduce);
}
