/**
[test]
reason = "Test that an invalid recursion option produces a parse error"

[test.results]
parsing-error = true
*/

a:1.{..+path+invalid};
