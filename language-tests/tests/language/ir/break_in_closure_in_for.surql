/**
[env]
namespace = true
database = true

[test]
reason = """
A BREAK inside a closure body is lexically outside the enclosing FOR: it raises the
control-flow error from the closure call, the FOR stops at that error, and no iteration wrote a row.
"""

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'brk' does not exist"
*/

FOR $i IN [1, 2] { [1].map(|$x| { BREAK }); CREATE brk SET i = $i; };
SELECT VALUE i FROM brk;
