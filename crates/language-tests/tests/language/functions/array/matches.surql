/**
[test]

[[test.results]]
value = "[false, true, false]"

[[test.results]]
value = "[true, false]"

[[test.results]]
value = "[false, true]"

*/
array::matches([0, 1, 2], 1);
array::matches([[], [0]], []);
array::matches([{id: "ohno:0"}, {id: "ohno:1"}], {id: "ohno:1"});
