/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
error = "Incorrect arguments for function set::is_subset_of(). Argument 1 was the wrong type. Expected `set` but found `3`"

*/

RETURN set::is_subset_of({,}, {,});
RETURN set::is_subset_of({1,2}, {1,2,3});
RETURN set::is_subset_of({1,2,3}, {1,2});
RETURN set::is_subset_of({1,2,4}, {1,2,3});
RETURN set::is_subset_of(3, true);
