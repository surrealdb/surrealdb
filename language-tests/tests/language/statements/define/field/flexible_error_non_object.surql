/**
[test]
reason = "Test that FLEXIBLE on non-object type throws parsing error"

[test.results]
parsing-error = """
FLEXIBLE can only be used with types containing object
  --> [18:36]
   |
18 | DEFINE FIELD a ON test TYPE number FLEXIBLE;
   |                                    ^^^^^^^^
"""

*/

-- FLEXIBLE on non-object type should error
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE number FLEXIBLE;

