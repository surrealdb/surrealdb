/**
[test]

[[test.results]]
value = "[false, false, false, true]"

[[test.results]]
value = "[false, false, false, true]"

[[test.results]]
value = "[false, false, true, false]"

[[test.results]]
value = "[false, false]"

[[test.results]]
value = "[false, false]"

*/

array::boolean_and([false, true, false, true], [false, false, true, true]);
array::boolean_and([0, 1, 0, 1], [0, 0, 1, 1]);
array::boolean_and([[], "", "1", {}], [[1], "", "1", /1/]);
array::boolean_and([true, false], [false]);
array::boolean_and([true, true], [false]);
