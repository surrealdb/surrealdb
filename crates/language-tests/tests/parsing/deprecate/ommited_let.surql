/**
[test]

[test.results]
parsing-error = """
Parameter declarations without `let` are deprecated.
  --> [14:1]
   |
14 | $a = 1
   | ^^^^^^ Replace with `let $a = ...` to keep the previous behavior.
"""

*/
$a = 1
