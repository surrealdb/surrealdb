/**
[test]

[[test.results]]
value = "{,}"

[[test.results]]
error = "Incorrect arguments for function set::union(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "{1, 2, 3, 4, 5, 6}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{1, 2, 3}"

*/

RETURN set::union({,}, {,});
RETURN set::union(3, true);
RETURN set::union({1,2,6}, {3,4,5,6});
RETURN set::union({1,2,3}, {,});
RETURN set::union({,}, {1,2,3});
RETURN set::union({1,2,3}, {1,2,3});
