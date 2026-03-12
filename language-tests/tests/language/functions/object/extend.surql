/**
[test]
reason = "Test extend function with incorrect arguments: compute-only shows 'function', new executor shows 'method'"

[[test.results]]
match = "$error = /Incorrect arguments for (?:function|method) (?:object::?)?extend\\(\\)\\. Argument 2 was the wrong type\\. Expected `object` but found `'9'`/"
error = true


[[test.results]]
value = "{ a: 9, b: 10 }"

*/
{ a: 9 }.extend('9');
{ a: 9 }.extend({ b: 10 });