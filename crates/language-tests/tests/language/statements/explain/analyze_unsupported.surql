/**
[test]

[test.results]
parsing-error = """
EXPLAIN ANALYZE is not yet supported
  --> [16:9]
   |
16 | EXPLAIN ANALYZE SELECT * FROM person;
   |         ^^^^^^^
"""

*/

-- ANALYZE should fail (not yet supported)
EXPLAIN ANALYZE SELECT * FROM person;
