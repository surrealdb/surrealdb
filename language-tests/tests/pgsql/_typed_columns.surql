/**
[test]
run = false
reason = "Setup only: a table whose id and columns are declared with scalar types, for the tests of text written to a typed column."
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD id ON item TYPE int;
DEFINE FIELD qty ON item TYPE int;
DEFINE FIELD price ON item TYPE decimal;
DEFINE FIELD active ON item TYPE bool;
