/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A block threads one child context through every statement it runs, so a `LET` that rebinds its own parameter accumulates nesting across the block's statements the same way a top-level `LET` chain does - but through the block binder, not the executor's top-level `LET` guard. Wrapping the chain in a block must not let it escape the cap. The accumulator is built at the cap by an accepted fold and then wrapped once more by a plain array literal (not a fold) bound with a second block `LET`, so it is the block `LET` binder - not the fold cap - that rejects it. The block's tail is `array::len`, which does not recurse, so the value never reaches the result-conversion guard: only the block `LET` binder can reject it here, on either execution engine."

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

*/

RETURN {
	LET $a = array::fold(array::range(0, 255), [], |$acc, $_| [$acc]);
	LET $a = [$a];
	array::len($a)
};
