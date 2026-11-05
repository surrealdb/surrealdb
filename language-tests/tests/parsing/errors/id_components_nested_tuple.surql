/**
[test]
reason = "Component names are accepted only on the outermost tuple."

[test.results]
parsing-error = """
Unexpected token `:`, expected a kind name
  --> [15:56]
   |
15 | ..., at: [lat: float, lon: float]];
   |              ^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, at: [lat: float, lon: float]];
