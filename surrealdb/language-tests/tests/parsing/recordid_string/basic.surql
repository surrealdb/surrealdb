/**
[test]

[[test.results]]
value = "a:b"

[[test.results]]
value = "a:b"

[[test.results]]
value = "a:b"

[[test.results]]
value = "a:['b']"

[[test.results]]
value = "a:['b']"

[[test.results]]
value = "a:['b']"

*/

r"a:b";
r"a\u{3a}b";
r"\u{61}\u{3a}\u{62}";
r"a:[\"b\"]";
r"a:['b']";
r"a:[\'\u{62}\']";
