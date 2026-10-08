//! Array functions
//!
//! Note: We use Kind::Any for array types since Kind::Array requires parameters.
//! The actual type checking is handled by the FromArgs trait.

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::exec::physical_expr::function::StreamingClosureEvaluator;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::{ClosureEvaluator, Value};
use crate::{define_pure_function, register_functions};

// Single array argument functions
define_pure_function!(ArrayDistinct, "array::distinct", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::distinct_walk, (array: Any) -> Any, crate::fnc::array::distinct);
define_pure_function!(ArrayFirst, "array::first", effects: EffectSet::PURE, rule: ReturnRule::ArrayElement(0), (array: Any) -> Any, crate::fnc::array::first);
define_pure_function!(ArrayFlatten, "array::flatten", effects: EffectSet::PURE, metered: crate::fnc::array::flatten_walk, (array: Any) -> Any, crate::fnc::array::flatten);
define_pure_function!(ArrayGroup, "array::group", effects: EffectSet::PURE, metered: crate::fnc::array::group_walk, (array: Any) -> Any, crate::fnc::array::group);
define_pure_function!(ArrayIsEmpty, "array::is_empty", effects: EffectSet::PURE, (array: Any) -> Bool, crate::fnc::array::is_empty);
define_pure_function!(ArrayLast, "array::last", effects: EffectSet::PURE, rule: ReturnRule::ArrayElement(0), (array: Any) -> Any, crate::fnc::array::last);
define_pure_function!(ArrayLen, "array::len", effects: EffectSet::PURE, (array: Any) -> Int, crate::fnc::array::len);
define_pure_function!(ArrayMax, "array::max", effects: EffectSet::PURE, rule: ReturnRule::ArrayElement(0), metered: crate::fnc::array::max_walk, (array: Any) -> Any, crate::fnc::array::max);
define_pure_function!(ArrayMin, "array::min", effects: EffectSet::PURE, rule: ReturnRule::ArrayElement(0), metered: crate::fnc::array::min_walk, (array: Any) -> Any, crate::fnc::array::min);
define_pure_function!(ArrayPop, "array::pop", effects: EffectSet::PURE, rule: ReturnRule::ArrayElement(0), (array: Any) -> Any, crate::fnc::array::pop);
define_pure_function!(ArrayReverse, "array::reverse", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), (array: Any) -> Any, crate::fnc::array::reverse);
// Shuffles with the thread rng, so it answers differently for one input and
// declares `EffectSet::ENTROPY`: a rule that folds a call at plan time or
// duplicates one across mentions must not touch it.
define_pure_function!(ArrayShuffle, "array::shuffle", effects: EffectSet::ENTROPY, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::shuffle_walk, (array: Any) -> Any, crate::fnc::array::shuffle);
define_pure_function!(ArraySort, "array::sort", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::sort_walk, (array: Any, ?order: Any) -> Any, crate::fnc::array::sort);
define_pure_function!(ArraySortNatural, "array::sort_natural", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::sort_natural_walk, (array: Any, ?order: Any) -> Any, crate::fnc::array::sort_natural);
define_pure_function!(ArraySortLexical, "array::sort_lexical", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::sort_lexical_walk, (array: Any, ?order: Any) -> Any, crate::fnc::array::sort_lexical);
define_pure_function!(ArraySortNaturalLexical, "array::sort_natural_lexical", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::sort_natural_lexical_walk, (array: Any, ?order: Any) -> Any, crate::fnc::array::sort_natural_lexical);
define_pure_function!(ArrayTranspose, "array::transpose", effects: EffectSet::PURE, metered: crate::fnc::array::transpose_walk, (array: Any) -> Any, crate::fnc::array::transpose);
define_pure_function!(ArrayBooleanNot, "array::boolean_not", effects: EffectSet::PURE, metered: crate::fnc::array::boolean_not_walk, (array: Any) -> Any, crate::fnc::array::boolean_not);

// Two argument array functions
define_pure_function!(ArrayAdd, "array::add", effects: EffectSet::PURE, metered: crate::fnc::array::add_walk, (array: Any, value: Any) -> Any, crate::fnc::array::add);
define_pure_function!(ArrayAppend, "array::append", effects: EffectSet::PURE, (array: Any, value: Any) -> Any, crate::fnc::array::append);
define_pure_function!(ArrayAt, "array::at", effects: EffectSet::PURE, rule: ReturnRule::ArrayElement(0), (array: Any, index: Int) -> Any, crate::fnc::array::at);
define_pure_function!(ArrayBooleanAnd, "array::boolean_and", effects: EffectSet::PURE, metered: crate::fnc::array::boolean_and_walk, (a: Any, b: Any) -> Any, crate::fnc::array::boolean_and);
define_pure_function!(ArrayBooleanOr, "array::boolean_or", effects: EffectSet::PURE, metered: crate::fnc::array::boolean_or_walk, (a: Any, b: Any) -> Any, crate::fnc::array::boolean_or);
define_pure_function!(ArrayBooleanXor, "array::boolean_xor", effects: EffectSet::PURE, metered: crate::fnc::array::boolean_xor_walk, (a: Any, b: Any) -> Any, crate::fnc::array::boolean_xor);
define_pure_function!(ArrayClump, "array::clump", effects: EffectSet::PURE, metered: crate::fnc::array::clump_walk, (array: Any, size: Int) -> Any, crate::fnc::array::clump);
define_pure_function!(ArrayCombine, "array::combine", effects: EffectSet::PURE, metered: crate::fnc::array::combine_walk, (a: Any, b: Any) -> Any, crate::fnc::array::combine);
define_pure_function!(ArrayComplement, "array::complement", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::complement_walk, (a: Any, b: Any) -> Any, crate::fnc::array::complement);
define_pure_function!(ArrayConcat, "array::concat", effects: EffectSet::PURE, metered: crate::fnc::array::concat_walk, (...arrays: Any) -> Any, crate::fnc::array::concat);
define_pure_function!(ArrayDifference, "array::difference", effects: EffectSet::PURE, metered: crate::fnc::array::difference_walk, (a: Any, b: Any) -> Any, crate::fnc::array::difference);
define_pure_function!(ArrayIntersect, "array::intersect", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::intersect_walk, (a: Any, b: Any) -> Any, crate::fnc::array::intersect);
define_pure_function!(ArrayJoin, "array::join", effects: EffectSet::PURE, metered: crate::fnc::array::join_walk, (array: Any, separator: String) -> String, crate::fnc::array::join);
define_pure_function!(ArrayLogicalAnd, "array::logical_and", effects: EffectSet::PURE, metered: crate::fnc::array::logical_and_walk, (a: Any, b: Any) -> Any, crate::fnc::array::logical_and);
define_pure_function!(ArrayLogicalOr, "array::logical_or", effects: EffectSet::PURE, metered: crate::fnc::array::logical_or_walk, (a: Any, b: Any) -> Any, crate::fnc::array::logical_or);
define_pure_function!(ArrayLogicalXor, "array::logical_xor", effects: EffectSet::PURE, metered: crate::fnc::array::logical_xor_walk, (a: Any, b: Any) -> Any, crate::fnc::array::logical_xor);
define_pure_function!(ArrayMatches, "array::matches", effects: EffectSet::PURE, metered: crate::fnc::array::matches_walk, (array: Any, value: Any) -> Any, crate::fnc::array::matches);
define_pure_function!(ArrayPrepend, "array::prepend", effects: EffectSet::PURE, (array: Any, value: Any) -> Any, crate::fnc::array::prepend);
define_pure_function!(ArrayPush, "array::push", effects: EffectSet::PURE, (array: Any, value: Any) -> Any, crate::fnc::array::push);
define_pure_function!(ArrayRemove, "array::remove", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), (array: Any, index: Int) -> Any, crate::fnc::array::remove);
define_pure_function!(ArrayRepeat, "array::repeat", effects: EffectSet::PURE, metered: crate::fnc::array::repeat_walk, (value: Any, count: Int) -> Any, crate::fnc::array::repeat);
define_pure_function!(ArrayUnion, "array::union", effects: EffectSet::PURE, metered: crate::fnc::array::union_walk, (a: Any, b: Any) -> Any, crate::fnc::array::union);
define_pure_function!(ArrayWindows, "array::windows", effects: EffectSet::PURE, metered: crate::fnc::array::windows_walk, (array: Any, size: Int) -> Any, crate::fnc::array::windows);
define_pure_function!(ArraySequence, "array::sequence", effects: EffectSet::PURE, metered: crate::fnc::array::sequence_walk, (start: Int, ?length: Int) -> Any, crate::fnc::array::sequence);

// Three argument array functions
define_pure_function!(ArrayFill, "array::fill", effects: EffectSet::PURE, metered: crate::fnc::array::fill_walk, (array: Any, value: Any, ?start: Any, ?end: Int) -> Any, crate::fnc::array::fill);
define_pure_function!(ArrayInsert, "array::insert", effects: EffectSet::PURE, (array: Any, value: Any, ?index: Int) -> Any, crate::fnc::array::insert);
define_pure_function!(ArrayRange, "array::range", effects: EffectSet::PURE, metered: crate::fnc::array::range_walk, (start: Any, ?end: Int) -> Any, crate::fnc::array::range);
define_pure_function!(ArraySlice, "array::slice", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), (array: Any, ?start: Any, ?end: Int) -> Any, crate::fnc::array::slice);
define_pure_function!(ArraySwap, "array::swap", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), (array: Any, i: Int, j: Int) -> Any, crate::fnc::array::swap);

// Sort submodule
define_pure_function!(ArraySortAsc, "array::sort::asc", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::sort::asc_walk, (array: Any) -> Any, crate::fnc::array::sort::asc);
define_pure_function!(ArraySortDesc, "array::sort::desc", effects: EffectSet::PURE, rule: ReturnRule::SameAsArg(0), metered: crate::fnc::array::sort::desc_walk, (array: Any) -> Any, crate::fnc::array::sort::desc);

// =========================================================================
// Closure-based array functions (require async execution with TreeStack)
// =========================================================================

/// A builtin that runs the closure it is handed over the elements of
/// an array. The closure is the final argument, written `|name: Kind|`, or
/// `?|name: Kind|` when it may be omitted; the macro declares its position
/// through [`ScalarFunction::closure_args`], which is what tells the effect
/// fold that a closure arriving there as a value runs a body the call cannot
/// see. A body written in the call is judged as a child of the call, and the
/// array argument never carries a body, so the declared effects are
/// those of the builtin alone: `PURE`.
macro_rules! define_array_closure_function {
	($struct_name:ident, $func_name:literal, $impl_path:path, $($arg:ident: $kind:ident,)+ ?|$carg:ident: $ckind:ident| => $ret:ident) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> EffectSet {
				EffectSet::PURE
			}

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
					let mut stack = TreeStack::new();
					stack
						.enter(|stk| async move {
							$impl_path((stk, ev), args).await
						})
						.finish()
						.await
				})
			}
		}
	};
	($struct_name:ident, $func_name:literal, $impl_path:path, $($arg:ident: $kind:ident,)+ |$carg:ident: $ckind:ident| => $ret:ident) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn effects(&self) -> EffectSet {
				EffectSet::PURE
			}

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
					let mut stack = TreeStack::new();
					stack
						.enter(|stk| async move {
							$impl_path((stk, ev), args).await
						})
						.finish()
						.await
				})
			}
		}
	};
}

// array::all - Check if all elements match a condition
define_array_closure_function!(ArrayAll, "array::all", crate::fnc::array::all, array: Any, ?|check: Any| => Any);

// array::any - Check if any element matches a condition
define_array_closure_function!(ArrayAny, "array::any", crate::fnc::array::any, array: Any, ?|check: Any| => Any);

// array::every - Alias for array::all
define_array_closure_function!(ArrayEvery, "array::every", crate::fnc::array::all, array: Any, ?|check: Any| => Any);

// array::filter - Filter elements by closure/value
define_array_closure_function!(ArrayFilter, "array::filter", crate::fnc::array::filter, array: Any, |check: Any| => Any);

// array::filter_index - Get indices of matching elements
define_array_closure_function!(ArrayFilterIndex, "array::filter_index", crate::fnc::array::filter_index, array: Any, |check: Any| => Any);

// array::find - Find first matching element
define_array_closure_function!(ArrayFind, "array::find", crate::fnc::array::find, array: Any, |check: Any| => Any);

// array::find_index - Find index of first matching element
define_array_closure_function!(ArrayFindIndex, "array::find_index", crate::fnc::array::find_index, array: Any, |check: Any| => Any);

// array::fold - Fold with accumulator and closure
define_array_closure_function!(ArrayFold, "array::fold", crate::fnc::array::fold, array: Any, init: Any, |mapper: Any| => Any);

// array::includes - Alias for array::any
define_array_closure_function!(ArrayIncludes, "array::includes", crate::fnc::array::any, array: Any, ?|check: Any| => Any);

// array::index_of - Alias for array::find_index
define_array_closure_function!(ArrayIndexOf, "array::index_of", crate::fnc::array::find_index, array: Any, |check: Any| => Any);

// array::map - Transform elements with closure
define_array_closure_function!(ArrayMap, "array::map", crate::fnc::array::map, array: Any, |mapper: Any| => Any);

// array::reduce - Reduce array with closure
define_array_closure_function!(ArrayReduce, "array::reduce", crate::fnc::array::reduce, array: Any, |mapper: Any| => Any);

// array::some - Alias for array::any
define_array_closure_function!(ArraySome, "array::some", crate::fnc::array::any, array: Any, ?|check: Any| => Any);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		ArrayAdd,
		ArrayAppend,
		ArrayAt,
		ArrayBooleanAnd,
		ArrayBooleanNot,
		ArrayBooleanOr,
		ArrayBooleanXor,
		ArrayClump,
		ArrayCombine,
		ArrayComplement,
		ArrayConcat,
		ArrayDifference,
		ArrayDistinct,
		ArrayFill,
		ArrayFirst,
		ArrayFlatten,
		ArrayGroup,
		ArrayInsert,
		ArrayIntersect,
		ArrayIsEmpty,
		ArrayJoin,
		ArrayLast,
		ArrayLen,
		ArrayLogicalAnd,
		ArrayLogicalOr,
		ArrayLogicalXor,
		ArrayMatches,
		ArrayMax,
		ArrayMin,
		ArrayPop,
		ArrayPrepend,
		ArrayPush,
		ArrayRange,
		ArrayRemove,
		ArrayRepeat,
		ArrayReverse,
		ArraySequence,
		ArrayShuffle,
		ArraySlice,
		ArraySort,
		ArraySortAsc,
		ArraySortDesc,
		ArraySortNatural,
		ArraySortLexical,
		ArraySortNaturalLexical,
		ArraySwap,
		ArrayTranspose,
		ArrayUnion,
		ArrayWindows,
	);

	// Register closure-based functions
	registry.register(ArrayAll);
	registry.register(ArrayAny);
	registry.register(ArrayEvery);
	registry.register(ArrayFilter);
	registry.register(ArrayFilterIndex);
	registry.register(ArrayFind);
	registry.register(ArrayFindIndex);
	registry.register(ArrayFold);
	registry.register(ArrayIncludes);
	registry.register(ArrayIndexOf);
	registry.register(ArrayMap);
	registry.register(ArrayReduce);
	registry.register(ArraySome);
}
