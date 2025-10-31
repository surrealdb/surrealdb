/**
[test]

[[test.results]]
value = "{,}"

[[test.results]]
value = "{3}"

[[test.results]]
value = "{'3'}"

*/

RETURN set::add({,}, {,});
RETURN set::add({,}, 3);
RETURN set::add({,}, '3');
-- RETURN set::add(3, true);
-- RETURN set::add({1,2}, 2);
-- RETURN set::add({1,2}, 3);
-- RETURN set::add({1,2}, [2,3]);
-- RETURN set::add({1,2,3}, {3,4});
-- RETURN set::add({1,2,3}, [3,4,5]);
