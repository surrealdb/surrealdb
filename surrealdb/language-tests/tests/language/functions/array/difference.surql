/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::difference(). Expected 2 arguments"

[[test.results]]
error = "Incorrect arguments for function array::difference(). Expected 2 arguments"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::difference(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
error = "Incorrect arguments for function array::difference(). Argument 2 was the wrong type. Expected `array` but found `true`"

[[test.results]]
value = "[1, 2, 5, 6]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }]"

[[test.results]]
value = "[[1], [2]]"

*/
array::difference();
array::difference([]);
array::difference([], []);
array::difference(3, true);
array::difference([3], true);
array::difference([1,2,3,4], [3,4,5,6]);
array::difference([{},{a:1},"a",1d,1f,2dec,3,[]], [{},{a:1},"a",1d,1f,2,3dec,[]]);
array::difference([{a: 1}], [{a: 2}]);
array::difference([[1]], [[2]]);
