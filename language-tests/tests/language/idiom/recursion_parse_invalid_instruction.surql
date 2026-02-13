/**
[test]
reason = "Test that an invalid recursion instruction produces a parse error"

[test.results]
parsing-error = true
*/

a:1.{..+invalid};
