/**
[test]

[[test.results]]
value = "{1, 2}"

[[test.results]]
error = "Incorrect arguments for function set::remove(). Argument 1 was the wrong type. Expected `set` but found `3`"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "{1,}"

[[test.results]]
value = "{1,}"

[[test.results]]
value = "{,}"


*/

RETURN set::remove({1, 2, 3}, 3);
RETURN set::remove(3, true);
RETURN set::remove({1, 2, 3}, 5);
RETURN set::remove({1, 2, 3}, [3]);
RETURN set::remove({1, 2, 3}, [2, 3]);
RETURN set::remove({1, 2, 3}, {2, 3});
RETURN set::remove({1, 2, 3}, [1, 2, 3]);
