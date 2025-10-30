/**
[test]
wip = true
reason = "Set literals are not yet supported"
*/

RETURN set::union({,}, {,});
RETURN set::union(3, true);
RETURN set::union({1,2,6}, {3,4,5,6});
RETURN set::union({1,2,3}, {,});
RETURN set::union({,}, {1,2,3});
RETURN set::union({1,2,3}, {1,2,3});
