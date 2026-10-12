/**
[test]
reason = "Component names are accepted only when the tuple is the whole TYPE of the id field, not one alternative of it."

[test.results]
parsing-error = """
component names are only allowed when they name the whole TYPE of the `id` field
  --> [15:33]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string] | none;
   |                                 ^^^^^^^^^^^^^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string] | none;
