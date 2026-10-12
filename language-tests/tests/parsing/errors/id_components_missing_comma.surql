/**
[test]
reason = "Named components are separated by commas."

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected `,` or `]`
  --> [15:47]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string time: datetime];
   |                                               ^^^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string time: datetime];
