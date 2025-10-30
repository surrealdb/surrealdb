/**
[test]
wip = true
reason = "Set literals are not yet supported"
*/

LET $set1 = {1, 2, 3};
LET $set2 = {3, 4, 5};

RETURN $set1.add(4);
RETURN $set1.remove(3);
RETURN $set1.union($set2);
RETURN $set1.intersect($set2);
RETURN $set1.difference($set2);
RETURN $set1.complement($set2);
RETURN $set1.contains(2);
RETURN $set1.contains(5);
RETURN $set1.len();
RETURN $set1.is_empty();

