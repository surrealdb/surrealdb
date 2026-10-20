/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A graph lookup step names its target by table, `?` or a subquery in
parentheses; a parameter in that place is not grammar. The lookup a parameter
was meant to express is written as a subquery or an edge filter, which
3588_graph_lookup_filtered_by_a_parameter holds.
"""
issue = 3588

[test.results]
parsing-error = """
Unexpected token `a parameter`, expected `?`, `(` or an identifier
  --> [26:22]
   |
26 | SELECT VALUE <-like<-$accountId FROM t;
   |                      ^^^^^^^^^^
"""
*/
LET $accountId = account:1;
SELECT VALUE <-like<-$accountId FROM t;
