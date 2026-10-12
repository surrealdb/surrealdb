/**
[test]
reason = "Component names are accepted only in the TYPE of the field id. Any other field reads the tuple as an ordinary tuple kind, so the name is an unexpected token where a kind is expected."

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected a kind name
  --> [15:37]
   |
15 | DEFINE FIELD value ON reading TYPE [city: string];
   |                                     ^^^^
"""

*/
DEFINE FIELD value ON reading TYPE [city: string];
