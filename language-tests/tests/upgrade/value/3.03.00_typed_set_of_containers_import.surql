/**
[test]
reason = "Setup for 3.03.00_typed_set_of_containers.surql: a released binary writes records whose field is a typed set of arrays."
run = false
*/
DEFINE TABLE tsc SCHEMALESS;
DEFINE FIELD s ON tsc TYPE set<array<int>>;
CREATE tsc:1 SET s = <set>[[1, 2], [3]] RETURN NONE;
CREATE tsc:2 SET s = <set>[[4]] RETURN NONE;
