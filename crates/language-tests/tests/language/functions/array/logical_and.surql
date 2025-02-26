/**
[test]

[[test.results]]
value = "[true, false, false, false]"

[[test.results]]
value = "[1, 0, false, 0]"

[[test.results]]
value = "[0, NULL]"

*/
array::logical_and([true, false, true, false], [true, true, false, false]);
array::logical_and([1, 0, 1, 0], [true, true, false, false]);
array::logical_and([0, 1], []);
