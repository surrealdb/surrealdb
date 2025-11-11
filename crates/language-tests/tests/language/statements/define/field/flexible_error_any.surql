/**
[test]
reason = "Test that FLEXIBLE on any type throws an error. TYPE ANY allows _any_ type, so FLEXIBLE is unnecessary."

[test.results]
parsing-error = """
FLEXIBLE can only be used with types containing object
  --> [18:33]
   |
18 | DEFINE FIELD a ON test TYPE any FLEXIBLE;
   |                                 ^^^^^^^^
"""

*/

-- FLEXIBLE on non-object type should error
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE any FLEXIBLE;

