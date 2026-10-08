/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An `IF` arm whose condition a constant `LET` settles false is planned like any
other arm, so a statement in it that planning refuses fails the `IF`, as main
fails it, whether the `LET` is the query's or the block's.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "4"
*/
LET $c = false;
IF $c { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 1 };
{ LET $d = false; IF $d { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 3 } };
{ LET $d = false; IF $d { SELECT * FROM t } ELSE { 4 } };
