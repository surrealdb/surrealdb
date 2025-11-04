/**
[test]
wip = true
reason = "Set literals are not yet supported"
*/

RETURN set::contains({,}, 3);
RETURN set::contains(3, true);
RETURN set::contains(type::set([1,2,3]), 2);
RETURN set::contains(type::set([1,2,3]), 5);
RETURN set::contains(type::set(['foo', 'bar', 'baz']), 'bar');
RETURN set::contains(type::set([1,2,3]), 'foo');

