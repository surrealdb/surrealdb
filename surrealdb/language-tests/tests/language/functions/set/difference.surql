/**
[test]

[[test.results]]
value = "{,}"

[[test.results]]
error = "Incorrect arguments for function set::difference(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "{1, 4}"

[[test.results]]
value = "{1, 2, 3, 4, 5}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{,}"

*/

RETURN set::difference({,}, {,});
RETURN set::difference(3, true);
RETURN set::difference({1,2,3}, {2,3,4});
RETURN set::difference({1,2,6}, {3,4,5,6});
RETURN set::difference({1,2,3}, {,});
RETURN set::difference({1,2,3}, {1,2,3});
