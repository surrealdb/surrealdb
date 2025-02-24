/**
[test]

[[test.results]]
value = "[true, true, true, false]"

[[test.results]]
value = "[1, true, 1, 0]"

[[test.results]]
value = "[0, 1]"

*/
array::logical_or([true, false, true, false], [true, true, false, false]);
array::logical_or([1, 0, 1, 0], [true, true, false, false]);
array::logical_or([0, 1], []);
