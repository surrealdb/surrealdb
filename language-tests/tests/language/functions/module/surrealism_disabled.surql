/**
[env.capabilities]
allow-experimental = []

[test]
reason = "Test that mod:: module syntax requires experimental capability when not enabled"

[test.results]
parsing-error = """
Experimental capability `surrealism` is not enabled
  --> [19:8]
   |
19 | RETURN mod::nonexistent();
   |        ^^^ Use of `mod::` is still experimental
"""
*/

-- Try to call a module function (requires experimental capability)
RETURN mod::nonexistent();
