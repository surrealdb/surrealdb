/**
[test]

[[test.results]]
error = "Incorrect arguments for function object::remove(). '9' cannot be used as a key. Please use a string instead."

[[test.results]]
value = "{ b: 'ten', c: 11 }"

[[test.results]]
value = "{ c: 11 }"

*/
{ a: 9, b: "ten", c: 11 }.remove(9);
{ a: 9, b: "ten", c: 11 }.remove('a');
{ a: 9, b: "ten", c: 11 }.remove(['a', 'b']);