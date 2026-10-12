/**
[test]
reason = "Component names are accepted only on the outermost tuple."

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected a kind name
  --> [15:53]
   |
15 | ...ing, at: [lat: float, lon: float]];
   |              ^^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, at: [lat: float, lon: float]];
