/**
[test]
reason = "`object::matches` is absent from the parser's builtin function path table, so a call is rejected at parse time with `Invalid function/constant path` and a span over the path. No path in the table is within the edit-distance cut-off, so no alternative is suggested."

[test.results]
parsing-error = """
Invalid function/constant path
  --> [15:1]
   |
15 | object::matches({ a: 1 }, { a: 1 });
   | ^^^^^^^^^^^^^^^
"""
*/

object::matches({ a: 1 }, { a: 1 });
