/**
[test]
wip = true
reason = "Set literals are not yet supported"

*/

RETURN set::remove({1, 2, 3}, 3);
RETURN set::remove(3, true);
RETURN set::remove({1, 2, 3}, 5);
RETURN set::remove({1, 2, 3}, [3]);
RETURN set::remove({1, 2, 3}, [2, 3]);
RETURN set::remove({1, 2, 3}, {2, 3});
RETURN set::remove({1, 2, 3}, [1, 2, 3]);
