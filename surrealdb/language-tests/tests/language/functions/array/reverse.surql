/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::reverse(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[4, 3, 3, 'text', 2, 1]"

[[test.results]]
value = "[4, [2, 3], 1]"

*/
array::reverse([]);
array::reverse(3);
array::reverse([1,2,"text",3,3,4]);
array::reverse([1,[2,3],4]);
