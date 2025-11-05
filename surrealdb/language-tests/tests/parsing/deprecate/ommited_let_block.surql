/**
[test]

[test.results]
parsing-error = """
Parameter declarations without `let` are deprecated.
  --> [15:2]
   |
15 | $a = 1;
   | ^^^^^^ Replace with `let $a = ...` to keep the previous behavior.
"""

*/
{
	$a = 1;
};
