/**
[test]

[[test.results]]
value = "[false, true, true, false]"

[[test.results]]
value = "[false, true, 1, 0]"

[[test.results]]
value = "[0, 1]"

*/
array::logical_xor([true, false, true, false], [true, true, false, false]);
array::logical_xor([1, 0, 1, 0], [true, true, false, false]);
array::logical_xor([0, 1], []);
