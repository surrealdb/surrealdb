/**
[test]
reason = "A time literal past 24:00:00 is a parse error"

[test.results]
parsing-error = """
Invalid time: "25:00:00"
  --> [14:6]
   |
14 | time'25:00:00';
   |      ^^^^^^^^
"""
*/
time'25:00:00';
