/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
type::field of a parameter with DESC in ORDER BY is a parse error.
"""
issue = 7296

[test.results]
parsing-error = """
Unexpected token `::`, expected Eof
  --> [22:37]
   |
22 | SELECT VALUE id FROM s ORDER BY type::field($f) DESC;
   |                                     ^^
"""
*/
SELECT VALUE id FROM s ORDER BY type::field($f) DESC;
