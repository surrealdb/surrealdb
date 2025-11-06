/**
[test]

[[test.results]]
value = "false"

[[test.results]]
error = "Incorrect arguments for function set::contains(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

RETURN set::contains({,}, 3);
RETURN set::contains(3, true);
RETURN set::contains(type::set([1,2,3]), 2);
RETURN set::contains(type::set([1,2,3]), 5);
RETURN set::contains(type::set(['foo', 'bar', 'baz']), 'bar');
RETURN set::contains(type::set([1,2,3]), 'foo');

