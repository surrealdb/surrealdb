/**
[test]
reason = "An interval literal with an unknown unit is a parse error"

[test.results]
parsing-error = """
Invalid interval: "1 fortnight"
  --> [14:5]
   |
14 | ivl'1 fortnight';
   |     ^^^^^^^^^^^
"""
*/
ivl'1 fortnight';
