/**
[env]
namespace = true
database = true

[test]
reason = "array::shuffle answers differently for one input, so inlining must not duplicate the call across the parameter's mentions"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, true]"

*/

-- A call the inline rule duplicates is evaluated once per mention. That is
-- only sound for a call that answers the same every time, so a shuffle has
-- to be classified as answering differently: the body mentions its parameter
-- twice, and the caller wrote one shuffle.

DEFINE FUNCTION fn::pair($x: any) { RETURN [$x, $x]; };

{
	LET $shuffled = fn::pair(array::shuffle([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
	RETURN [$shuffled[0].len(), $shuffled[0] = $shuffled[1]];
};
