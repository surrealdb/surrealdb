/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A parameter as a graph step table (->$edge->$table).
"""
issue = 7167

[test.results]
parsing-error = """
Unexpected token `a parameter`, expected `?`, `(` or an identifier
  --> [28:13]
   |
28 | SELECT *, ->$edge->$table2 AS e FROM ONLY $item1;
   |             ^^^^^
"""
*/
LET $table1 = type::table("t1");
LET $edge = type::table("hs");
LET $table2 = type::table("t2");
LET $item1 = CREATE ONLY $table1;
LET $item2 = CREATE ONLY $table2;
RELATE $item1->$edge->$item2;
SELECT *, ->$edge->$table2 AS e FROM ONLY $item1;
