/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
error = "Incorrect arguments for function set::is_disjoint(). Argument 1 was the wrong type. Expected `set` but found `3`"

*/

RETURN set::is_disjoint({,}, {,});
RETURN set::is_disjoint({1,2,3}, {2,3,4});
RETURN set::is_disjoint({1,2,3}, {4,5,6});
RETURN set::is_disjoint({1,}, {1,2,3});
RETURN set::is_disjoint(3, true);
