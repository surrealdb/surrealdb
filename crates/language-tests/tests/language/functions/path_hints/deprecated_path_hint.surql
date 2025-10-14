/**
[test]

[test.results]
parsing-error = """
Invalid function/constant path, did you maybe mean `type::record`
  --> [16:1]
   |
15 | type::thing("person", "one");
   | ^^^^^^^^^^^
"""

*/

type::thing("person", "one");