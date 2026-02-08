/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "20"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event throw_it: An error occurred: Thrown error!"

[[test.results]]
value = "20"

*/

DEFINE EVENT throw_it ON person THEN { CREATE blah; };
CREATE |person:20| RETURN NONE;
count(SELECT * FROM blah);

REMOVE EVENT throw_it ON person;
DEFINE EVENT throw_it ON person THEN {
    CREATE blah;
    THROW "Thrown error!";
};
CREATE |person:10| RETURN NONE;
count(SELECT * FROM blah);
