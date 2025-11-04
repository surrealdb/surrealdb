/**
[test]
wip = true
reason = "Set literals are not yet supported"

*/

RETURN set::add({,}, 3);
RETURN set::add(3, true);
RETURN set::add({1,2}, 2);
RETURN set::add({1,2}, 3);
RETURN set::add({1,2}, [2,3]);
RETURN set::add({1,2,3}, {3,4});
RETURN set::add({1,2,3}, [3,4,5]);
