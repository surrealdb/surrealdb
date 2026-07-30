/**
[test]
reason = "`matches` is a method of `string` and `array` only, so the method form on an object is rejected by method dispatch at execution time. The wording differs between the legacy compute executor and the streaming executor, hence the alternation."

[[test.results]]
match = "$error = /(?:There was a problem running the matches\\(\\) function\\. no such method found for the object type|Method 'matches' cannot be called on value of type 'object')/"
error = true
*/

{ a: 1 }.matches({ a: 1 });
