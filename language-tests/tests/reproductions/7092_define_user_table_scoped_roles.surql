/**
[env]
namespace = true
database = true

[test]
reason = "Table-scoped roles on DEFINE USER are a feature request, not a defect: a user's roles are OWNER, EDITOR and VIEWER at the level the user is defined on. The clause the request proposes is refused by the parser, and table-level access is expressed with a record access method and table PERMISSIONS."
issue = 7092

[test.results]
parsing-error = """
Unexpected token `PERMISSIONS`, expected Eof
  --> [21:57]
   |
21 | ...password' PERMISSIONS EDITOR ON TABLE account, EDITOR ON TABLE credential;
   |              ^^^^^^^^^^^
"""

*/

DEFINE USER svc_account ON DATABASE PASSWORD 'password' PERMISSIONS EDITOR ON TABLE account, EDITOR ON TABLE credential;
