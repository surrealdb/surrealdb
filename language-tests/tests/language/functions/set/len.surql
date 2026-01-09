/**
[test]
reason = "Test set::len function for getting set size"

[[test.results]]
value = "0"

[[test.results]]
error = "Incorrect arguments for function set::len(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "3"

[[test.results]]
value = "5"

[[test.results]]
value = "3"

*/

RETURN set::len(type::set([]));
RETURN set::len(3);
RETURN set::len(type::set([1,2,3]));
RETURN set::len(type::set([1,2,3,4,5]));
RETURN set::len(type::set([1,1,2,2,3,3]));

