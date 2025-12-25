/**
[test]
reason = "Test set::is_empty function for checking if set is empty"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function set::is_empty(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

*/

RETURN set::is_empty(type::set([]));
RETURN set::is_empty(3);
RETURN set::is_empty(type::set([1]));
RETURN set::is_empty(type::set([1,2,3]));

