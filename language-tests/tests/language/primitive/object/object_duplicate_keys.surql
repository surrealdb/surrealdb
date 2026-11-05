/**
[test]
reason = "An object literal that repeats a key keeps the last occurrence, and every entry's expression is evaluated even when a later entry overwrites it. Object is a sorted map built after all entries are evaluated, so both properties have to be upheld by that construction rather than falling out of insert-per-entry ordering."

[[test.results]]
value = "{ a: 3, b: 2 }"

[[test.results]]
value = "{ a: 1 }"

[[test.results]]
error = "An error occurred: first"

*/

{ a: 1, b: 2, a: 3 };
{ a: 2, a: 1 };
{ a: (THROW 'first'), a: 2 }
