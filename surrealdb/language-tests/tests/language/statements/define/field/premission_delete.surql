/**
[test]

[test.results]
parsing-error = """
Can't define permission DELETE for fields
"""

*/
DEFINE FIELD foo ON TABLE bar PERMISSIONS FOR DELETE NONE;
