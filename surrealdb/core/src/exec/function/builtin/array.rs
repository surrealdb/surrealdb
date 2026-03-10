//! Array functions
//!
//! Note: We use Kind::Any for array types since Kind::Array requires parameters.
//! The actual type checking is handled by the FromArgs trait.

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::fnc::args::FromArgs;
use crate::val::Value;
use crate::{define_pure_function, register_functions};

// Single array argument functions
define_pure_function!(ArrayDistinct, "array::distinct", (array: Any) -> Any, crate::fnc::array::distinct);
define_pure_function!(ArrayFirst, "array::first", (array: Any) -> Any, crate::fnc::array::first);
define_pure_function!(ArrayFlatten, "array::flatten", (array: Any) -> Any, crate::fnc::array::flatten);
define_pure_function!(ArrayGroup, "array::group", (array: Any) -> Any, crate::fnc::array::group);
define_pure_function!(ArrayIsEmpty, "array::is_empty", (array: Any) -> Bool, crate::fnc::array::is_empty);
define_pure_function!(ArrayLast, "array::last", (array: Any) -> Any, crate::fnc::array::last);
define_pure_function!(ArrayLen, "array::len", (array: Any) -> Int, crate::fnc::array::len);
define_pure_function!(ArrayMax, "array::max", (array: Any) -> Any, crate::fnc::array::max);
define_pure_function!(ArrayMin, "array::min", (array: Any) -> Any, crate::fnc::array::min);
define_pure_function!(ArrayPop, "array::pop", (array: Any) -> Any, crate::fnc::array::pop);
define_pure_function!(ArrayReverse, "array::reverse", (array: Any) -> Any, crate::fnc::array::reverse);
define_pure_function!(ArrayShuffle, "array::shuffle", (array: Any) -> Any, crate::fnc::array::shuffle);
define_pure_function!(ArraySort, "array::sort", (array: Any) -> Any, crate::fnc::array::sort);
define_pure_function!(ArraySortNatural, "array::sort_natural", (array: Any) -> Any, crate::fnc::array::sort_natural);
define_pure_function!(ArraySortLexical, "array::sort_lexical", (array: Any) -> Any, crate::fnc::array::sort_lexical);
define_pure_function!(ArraySortNaturalLexical, "array::sort_natural_lexical", (array: Any) -> Any, crate::fnc::array::sort_natural_lexical);
define_pure_function!(ArrayTranspose, "array::transpose", (array: Any) -> Any, crate::fnc::array::transpose);
define_pure_function!(ArrayBooleanNot, "array::boolean_not", (array: Any) -> Any, crate::fnc::array::boolean_not);

// Two argument array functions
define_pure_function!(ArrayAdd, "array::add", (array: Any, value: Any) -> Any, crate::fnc::array::add);
define_pure_function!(ArrayAppend, "array::append", (array: Any, value: Any) -> Any, crate::fnc::array::append);
define_pure_function!(ArrayAt, "array::at", (array: Any, index: Int) -> Any, crate::fnc::array::at);
define_pure_function!(ArrayBooleanAnd, "array::boolean_and", (a: Any, b: Any) -> Any, crate::fnc::array::boolean_and);
define_pure_function!(ArrayBooleanOr, "array::boolean_or", (a: Any, b: Any) -> Any, crate::fnc::array::boolean_or);
define_pure_function!(ArrayBooleanXor, "array::boolean_xor", (a: Any, b: Any) -> Any, crate::fnc::array::boolean_xor);
define_pure_function!(ArrayClump, "array::clump", (array: Any, size: Int) -> Any, crate::fnc::array::clump);
define_pure_function!(ArrayCombine, "array::combine", (a: Any, b: Any) -> Any, crate::fnc::array::combine);
define_pure_function!(ArrayComplement, "array::complement", (a: Any, b: Any) -> Any, crate::fnc::array::complement);
define_pure_function!(ArrayConcat, "array::concat", (a: Any, b: Any) -> Any, crate::fnc::array::concat);
define_pure_function!(ArrayDifference, "array::difference", (a: Any, b: Any) -> Any, crate::fnc::array::difference);
define_pure_function!(ArrayIntersect, "array::intersect", (a: Any, b: Any) -> Any, crate::fnc::array::intersect);
define_pure_function!(ArrayJoin, "array::join", (array: Any, separator: String) -> String, crate::fnc::array::join);
define_pure_function!(ArrayLogicalAnd, "array::logical_and", (a: Any, b: Any) -> Any, crate::fnc::array::logical_and);
define_pure_function!(ArrayLogicalOr, "array::logical_or", (a: Any, b: Any) -> Any, crate::fnc::array::logical_or);
define_pure_function!(ArrayLogicalXor, "array::logical_xor", (a: Any, b: Any) -> Any, crate::fnc::array::logical_xor);
define_pure_function!(ArrayMatches, "array::matches", (array: Any, value: Any) -> Any, crate::fnc::array::matches);
define_pure_function!(ArrayPrepend, "array::prepend", (array: Any, value: Any) -> Any, crate::fnc::array::prepend);
define_pure_function!(ArrayPush, "array::push", (array: Any, value: Any) -> Any, crate::fnc::array::push);
define_pure_function!(ArrayRemove, "array::remove", (array: Any, index: Int) -> Any, crate::fnc::array::remove);
define_pure_function!(ArrayRepeat, "array::repeat", (value: Any, count: Int) -> Any, crate::fnc::array::repeat);
define_pure_function!(ArrayUnion, "array::union", (a: Any, b: Any) -> Any, crate::fnc::array::union);
define_pure_function!(ArrayWindows, "array::windows", (array: Any, size: Int) -> Any, crate::fnc::array::windows);
define_pure_function!(ArraySequence, "array::sequence", (start: Int, end: Int) -> Any, crate::fnc::array::sequence);

// Three argument array functions
define_pure_function!(ArrayFill, "array::fill", (array: Any, value: Any, ?index: Int) -> Any, crate::fnc::array::fill);
define_pure_function!(ArrayInsert, "array::insert", (array: Any, value: Any, index: Int) -> Any, crate::fnc::array::insert);
define_pure_function!(ArrayRange, "array::range", (array: Any, start: Int, end: Int) -> Any, crate::fnc::array::range);
define_pure_function!(ArraySlice, "array::slice", (array: Any, start: Int, ?length: Int) -> Any, crate::fnc::array::slice);
define_pure_function!(ArraySwap, "array::swap", (array: Any, i: Int, j: Int) -> Any, crate::fnc::array::swap);

// Sort submodule
define_pure_function!(ArraySortAsc, "array::sort::asc", (array: Any) -> Any, crate::fnc::array::sort::asc);
define_pure_function!(ArraySortDesc, "array::sort::desc", (array: Any) -> Any, crate::fnc::array::sort::desc);

// =========================================================================
// Closure-based array functions (require async execution with TreeStack)
// =========================================================================

/// Helper macro for creating closure-based array functions
macro_rules! define_array_closure_function {
	($struct_name:ident, $func_name:literal, $impl_path:path, $($arg:ident: $kind:ident),+ => $ret:ident) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> Signature {
				Signature::new()
					$(.arg(stringify!($arg), Kind::$kind))+
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
					let frozen = ctx.exec_ctx.ctx();
					let opt = ctx.exec_ctx.options();
					// Note: CursorDoc is not available in the streaming executor context
					let doc = None;
					let mut stack = TreeStack::new();
					stack
						.enter(|stk| async move {
							$impl_path((stk, frozen, opt, doc), args).await
						})
						.finish()
						.await
				})
			}
		}
	};
}

// array::all - Check if all elements match a condition
define_array_closure_function!(ArrayAll, "array::all", crate::fnc::array::all, array: Any, check: Any => Any);

// array::any - Check if any element matches a condition
define_array_closure_function!(ArrayAny, "array::any", crate::fnc::array::any, array: Any, check: Any => Any);

// array::filter - Filter elements by closure/value
define_array_closure_function!(ArrayFilter, "array::filter", crate::fnc::array::filter, array: Any, check: Any => Any);

// array::filter_index - Get indices of matching elements
define_array_closure_function!(ArrayFilterIndex, "array::filter_index", crate::fnc::array::filter_index, array: Any, check: Any => Any);

// array::find - Find first matching element
define_array_closure_function!(ArrayFind, "array::find", crate::fnc::array::find, array: Any, check: Any => Any);

// array::find_index - Find index of first matching element
define_array_closure_function!(ArrayFindIndex, "array::find_index", crate::fnc::array::find_index, array: Any, check: Any => Any);

// array::fold - Fold with accumulator and closure
define_array_closure_function!(ArrayFold, "array::fold", crate::fnc::array::fold, array: Any, init: Any, mapper: Any => Any);

// array::map - Transform elements with closure
define_array_closure_function!(ArrayMap, "array::map", crate::fnc::array::map, array: Any, mapper: Any => Any);

// array::reduce - Reduce array with closure
define_array_closure_function!(ArrayReduce, "array::reduce", crate::fnc::array::reduce, array: Any, mapper: Any => Any);

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
	registry.register(ArrayFilter);
	registry.register(ArrayFilterIndex);
	registry.register(ArrayFind);
	registry.register(ArrayFindIndex);
	registry.register(ArrayFold);
	registry.register(ArrayMap);
	registry.register(ArrayReduce);
}
