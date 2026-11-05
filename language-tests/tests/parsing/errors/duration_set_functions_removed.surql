/**
[test]
reason = "duration::set_* paths were advertised by the parser but never implemented; they are now rejected at parse time"

[test.results]
parsing-error = """
Invalid function/constant path
  --> [14:8]
   |
14 | RETURN duration::set_hour(1h30m, 5);
   |        ^^^^^^^^^^^^^^^^^^
"""
*/
RETURN duration::set_hour(1h30m, 5);
