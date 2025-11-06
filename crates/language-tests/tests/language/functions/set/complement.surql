/**
[test]

[[test.results]]
value = "{,}"

[[test.results]]
error = "Incorrect arguments for function set::complement(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "{1,}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{,}"

*/

RETURN set::complement({,}, {,});
RETURN set::complement(3, true);
RETURN set::complement({1,2,3}, {2,3,4});
RETURN set::complement({1,2,6}, {3,4,5,6});
RETURN set::complement({1,2,3}, {,});
RETURN set::complement({1,2,3}, {1,2,3});
