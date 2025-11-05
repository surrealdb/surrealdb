/**
[test]

[[test.results]]
value = "[false, true, true, true]"

[[test.results]]
value = "[false, true, true, true]"

[[test.results]]
value = "[false, true, true, true]"

[[test.results]]
value = "[true, false]"

[[test.results]]
value = "[true, true]"

*/

array::boolean_or([false, true, false, true], [false, false, true, true]);
array::boolean_or([0, 1, 0, 1], [0, 0, 1, 1]);
array::boolean_or(["", "1", "", "1"], ["", "", "1", "1"]);
array::boolean_or([true, false], [false]);
array::boolean_or([true, true], [false]);
