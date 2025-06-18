/**
[test]

[[test.results]]
error = "Incorrect arguments for function extend(). Argument 2 was the wrong type. Expected `object` but found `'9'`"


[[test.results]]
value = "{ a: 9, b: 10 }"

*/
{ a: 9 }.extend('9');
{ a: 9 }.extend({ b: 10 });