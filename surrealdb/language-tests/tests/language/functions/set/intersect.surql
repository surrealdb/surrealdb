/**
[test]
wip = true
reason = "Set literals are not yet supported"
*/

RETURN set::intersect({,}, {,});
RETURN set::intersect(3, true);
RETURN set::intersect({1,2,3}, {2,3,4});
RETURN set::intersect({1,2,6}, {3,4,5,6});
RETURN set::intersect({1,2,3}, {,});
RETURN set::intersect({1,2,3}, {1,2,3});

