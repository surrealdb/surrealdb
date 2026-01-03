/**
[test]
reason = "Test EXPLAIN FORMAT JSON should fail (not yet supported)"

[test.results]
parsing-error = """
EXPLAIN FORMAT JSON is not yet supported
  --> [18:16]
   |
18 | EXPLAIN FORMAT JSON SELECT * FROM person;
   |                ^^^^
"""


*/

-- FORMAT JSON should fail (not yet supported)
EXPLAIN FORMAT JSON SELECT * FROM person;
