/**
[test]
reason = "A date literal outside the calendar is a parse error"

[test.results]
parsing-error = """
Invalid date "2026-13-45": input is out of range
  --> [14:4]
   |
14 | pd'2026-13-45';
   |    ^^^^^^^^^^
"""
*/
pd'2026-13-45';
