/**
[test]
reason = "`matches` is a method of `string` and `array` only, so the method form on an object is rejected by method dispatch at execution time, with the same wording under every executor."

[[test.results]]
error = "There was a problem running the matches() function. no such method found for the object type"
*/

{ a: 1 }.matches({ a: 1 });
