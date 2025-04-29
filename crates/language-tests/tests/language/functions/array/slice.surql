/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::slice(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2, 'text', 3, 3, 4]"

[[test.results]]
value = "[2, 'text', 3, 3, 4]"

[[test.results]]
value = "[2, 'text']"

[[test.results]]
value = "[2, 'text', 3, 3, 4]"

[[test.results]]
value = "[3, 3, 4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[3, 3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 'text']"

[[test.results]]
value = "[2, 'text']"

[[test.results]]
value = "[4]"

*/

array::slice([]);
array::slice(3);
array::slice([1,2,"text",3,3,4]);
array::slice([1,2,"text",3,3,4], 1,);
array::slice([1,2,"text",3,3,4], 1,2);
array::slice([1,2,"text",3,3,4], 1,9999999999999);
array::slice([1,2,"text",3,3,4], 3);
array::slice([1,2,"text",3,3,4], 999999999999);
array::slice([1,2,"text",3,3,4], 3, -1);
array::slice([1,2,"text",3,3,4], 5, -3);
array::slice([1,2,"text",3,3,4], 0, -3);
array::slice([1,2,"text",3,3,4], 1, -3);
array::slice([1,2,"text",3,3,4], -1);
