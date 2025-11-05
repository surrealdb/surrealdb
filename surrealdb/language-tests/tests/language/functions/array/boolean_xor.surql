/**
[test]

[[test.results]]
value = "[false, true, true, false]"

[[test.results]]
value = "[false, true, true, false]"

*/
array::boolean_xor([false, true, false, true], [false, false, true, true]);
array::boolean_xor([0, "1", "", 1h], [[], {}, [1], {a: 1}]);
