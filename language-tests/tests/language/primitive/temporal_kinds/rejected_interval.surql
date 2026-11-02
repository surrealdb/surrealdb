/**
[test]
reason = "An interval literal with an unknown unit is a parse error"

[test.results]
parsing-error = """
Invalid interval: "1 fortnight"
  --> [14:10]
   |
14 | interval'1 fortnight';
   |          ^^^^^^^^^^^
"""
*/
interval'1 fortnight';
