/**
[test]
reason = "A date literal outside the calendar is a parse error"

[test.results]
parsing-error = """
Invalid date "2026-13-45": input is out of range
  --> [14:6]
   |
14 | date'2026-13-45';
   |      ^^^^^^^^^^
"""
*/
date'2026-13-45';
