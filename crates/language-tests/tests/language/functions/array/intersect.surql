/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::intersect(). Expected 2 arguments"

[[test.results]]
error = "Incorrect arguments for function array::intersect(). Expected 2 arguments"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::intersect(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
error = "Incorrect arguments for function array::intersect(). Argument 2 was the wrong type. Expected `array` but found `true`"

[[test.results]]
value = "[3, 4]"

[[test.results]]
value = "[{  }, { a: 1 }, 'a', 1d, 1f, 2dec, 3, []]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/
array::intersect();
array::intersect([]);
array::intersect([], []);
array::intersect(3, true);
array::intersect([3], true);
array::intersect([1,2,3,4], [3,4,5,6]);
array::intersect([{},{a:1},"a",1d,1f,2dec,3,[]], [{},{a:1},"a",1d,1f,2,3dec,[]]);
array::intersect([{a: 1}], [{a: 2}]);
array::intersect([[1]], [[2]]);
