/**
[test]
wip = true

[test.results]
parsing-error = """
Variable declaration without `let` is deprecated
  --> [14:1]
   |
14 | $a = 1
   | ^^^^^^ replace with `let $a = ..`
"""

*/
$a = 1
