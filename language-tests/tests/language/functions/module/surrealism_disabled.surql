/**
[test]
reason = "Test that mod:: module syntax requires experimental capability when not enabled"

[test.results]
parsing-error = """
Experimental capability `surrealism` is not enabled
  --> [21:8]
   |
21 | RETURN mod::nonexistent();
   |        ^^^ Use of `mod::` is still experimental
"""

[env]
namespace = "test"
database = "test"

*/

-- Try to call a module function (requires experimental capability)
RETURN mod::nonexistent();
