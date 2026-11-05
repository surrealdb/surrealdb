/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
UNION between SELECT statements.
"""
issue = 6993

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected Eof
  --> [24:23]
   |
24 | SELECT VALUE v FROM x UNION SELECT VALUE v FROM y;
   |                       ^^^^^
"""
*/
CREATE x:1 SET v = 1;
CREATE y:1 SET v = 2;
SELECT VALUE v FROM x UNION SELECT VALUE v FROM y;
