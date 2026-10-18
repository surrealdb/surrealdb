/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
type::fields in ORDER BY is a parse error.
"""
issue = 7296

[test.results]
parsing-error = """
Unexpected token `::`, expected Eof
  --> [22:37]
   |
22 | SELECT VALUE id FROM s ORDER BY type::fields(['a', 'b']);
   |                                     ^^
"""
*/
SELECT VALUE id FROM s ORDER BY type::fields(['a', 'b']);
