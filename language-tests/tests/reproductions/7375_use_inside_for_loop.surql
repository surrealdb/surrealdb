/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
USE inside a FOR loop body.
"""
issue = 7375

[test.results]
parsing-error = """
Unexpected token `NAMESPACE` expected delimiter `}`
  --> [26:24]
   |
26 | FOR $i IN [1, 2] { USE NS a DB b; };
   |                        ^^
  --> [26:18]
   |
26 | FOR $i IN [1, 2] { USE NS a DB b; };
   |                  ^ expected this delimiter to close
"""
*/
FOR $i IN [1, 2] { USE NS a DB b; };
