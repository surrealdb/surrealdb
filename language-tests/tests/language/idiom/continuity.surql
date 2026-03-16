/**
[test]

[[test.results]]
value = "['a', 1]"

[[test.results]]
value = "['a', 'b']"

*/

// Idiom as part of a continues chain.
[{ a: ["a","b"]},{ a: [1,2]}].a[0];

// `()` should break up the chain.
([{ a: ["a","b"]},{ a: [1,2]}].a)[0];
