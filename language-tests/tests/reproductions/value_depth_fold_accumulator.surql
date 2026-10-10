/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A value's nesting depth must be bounded where it is built, not only where it is stored. `array::fold` / `array::reduce` (and the `set::` equivalents) thread a caller-controlled accumulator through a flat loop, so a closure that wraps it each turn grows the value's depth without bound - the per-compute `dive` budget resets every iteration, and the value need never be stored, so neither the compute-depth cap nor the storage-write guard sees it. Without a bound the next iteration's clone of the accumulator overflows the stack, aborting the process under `panic = 'abort'`. The fold/reduce accumulator is depth-capped each iteration, so the deep build is rejected with `ValueTooDeep` instead of aborting. The accumulator can be an array, a set element, or a closure that captures itself, so the cap walks all three. A `FOR` loop is not a vector: a body `LET` is scoped to the iteration and does not accumulate."

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "1"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

RETURN array::fold(array::range(0, 200), [], |$a, $_| array::fold(array::range(0, 200), $a, |$b, $_| [$b]));
-- `reduce` / `set::fold` / `set::reduce` are wrapped in `array::len` (which does
-- not recurse into its argument) so the accumulator guard is the only thing that
-- can error: without it the accumulator would build past the cap and `array::len`
-- would return a number, not error. A bare `RETURN` of the deep value would be
-- caught by the result conversion instead, masking a reverted accumulator guard.
RETURN array::len(array::reduce(array::range(0, 300), |$a, $b| [$a]));
RETURN array::len(set::fold(<set> array::range(0, 300), [], |$a, $_| [$a]));
RETURN array::len(set::reduce(<set> array::range(0, 300), |$a, $_| [$a]));
RETURN array::fold(array::range(0, 300), 0, |$a, $_| |$x| $a);
-- The boundary sits exactly at the cap, and the check runs after each wrap, not
-- before: 255 wraps reach the cap and return (len 1), 256 wraps trip it. The
-- `array::len` wrapper (which does not recurse into its argument) is what pins
-- the "after each wrap" placement - were the check moved before the wrap, the
-- 256 case would leak a value one past the cap out of the fold and `array::len`
-- would return 1 instead of erroring.
RETURN array::len(array::fold(array::range(0, 255), [], |$a, $_| [$a]));
RETURN array::len(array::fold(array::range(0, 256), [], |$a, $_| [$a]));
RETURN array::fold([1, 2], [], |$a, $_| [$a]);
RETURN array::fold([1, 2, 3], 0, |$a, $_| $a + 1);
LET $acc = [];
FOR $_ IN [1, 2, 3, 4, 5] { LET $acc = [$acc]; };
RETURN $acc;
