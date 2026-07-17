/**
[test]

[[test.results]]
value = "8"

[[test.results]]
value = "'HELLO'"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "'fallback'"

[[test.results]]
error = "Incorrect arguments for function value::chain(). Argument 2 was the wrong type. Expected `function` but found `2`"
*/
value::chain(4, |$v| $v * 2);
value::chain("hello", |$v| string::uppercase($v));
value::chain({ a: 1, b: 2 }, |$v| $v.a + $v.b);
value::chain([1, 2, 3], |$v| $v.len());
value::chain(NONE, |$v| $v ?? "fallback");
value::chain(4, 2);
