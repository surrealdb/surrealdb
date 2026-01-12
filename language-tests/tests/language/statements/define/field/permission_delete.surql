/**
[test]

[test.results]
parsing-error = """
Can't define permission DELETE for fields
  --> [14:47]
   |
14 | DEFINE FIELD foo ON TABLE bar PERMISSIONS FOR DELETE NONE;
   |                                               ^^^^^^
"""

*/
DEFINE FIELD foo ON TABLE bar PERMISSIONS FOR DELETE NONE;
