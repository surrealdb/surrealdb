/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::complement(). Expected 2 arguments"

[[test.results]]
error = "Incorrect arguments for function array::complement(). Expected 2 arguments"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::complement(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1 }]"

[[test.results]]
value = "[[1]]"

*/

array::complement();
array::complement([]);
array::complement([], []);
array::complement(3, true);
array::complement([1,2,3,4], [3,4,5,6]);
array::complement([{},{a:1},"a",1d,1f,2dec,3,[]], [{},{a:1},"a",1d,1f,2,3dec,[]]);
array::complement([{a: 1}], [{a: 2}]);
array::complement([[1]], [[2]]);
