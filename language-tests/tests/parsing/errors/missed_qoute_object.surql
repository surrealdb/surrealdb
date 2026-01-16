/**
[test]

[test.results]
parsing-error = """
Unexpected token `a strand` expected delimiter `}`
  --> [18:15]
   |
18 | {"id:0,"method":"query","params": ["SLEEP 30s"]}
   |               ^^^
  --> [18:1]
   |
18 | {"id:0,"method":"query","params": ["SLEEP 30s"]}
   | ^ expected this delimiter to close
"""

*/
{"id:0,"method":"query","params": ["SLEEP 30s"]}
