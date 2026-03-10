/**
[test]

[[test.results]]
value = "{,}"

[[test.results]]
error = "Incorrect arguments for function set::intersect(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "{2, 3}"

[[test.results]]
value = "{6,}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{1, 2, 3}"

*/

RETURN set::intersect({,}, {,});
RETURN set::intersect(3, true);
RETURN set::intersect({1,2,3}, {2,3,4});
RETURN set::intersect({1,2,6}, {3,4,5,6});
RETURN set::intersect({1,2,3}, {,});
RETURN set::intersect({1,2,3}, {1,2,3});

