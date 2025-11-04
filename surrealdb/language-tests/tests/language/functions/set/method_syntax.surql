/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{1, 2, 3, 4}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "{1, 2, 3, 4, 5}"

[[test.results]]
value = "{3,}"

[[test.results]]
value = "{1, 2, 4, 5}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "3"

[[test.results]]
value = "false"

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

