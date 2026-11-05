/**
[test]
reason = "A SCHEMAFULL table's bigdecimal field is held in a packed row's bigdecimal column: values of every size, sign and scale are written and read back exactly, filtered and updated in place."

*/

DEFINE TABLE pbd SCHEMAFULL;
DEFINE FIELD v ON pbd TYPE bigdecimal;
DEFINE FIELD w ON pbd TYPE option<bigdecimal>;
DEFINE FIELD n ON pbd TYPE int;

CREATE pbd:a SET n = 1, v = 1.50bigdec;
CREATE pbd:b SET n = 2, v = -12345678901234567890123456789012345678901234567890.123456789bigdec, w = 0.000bigdec;
CREATE pbd:c SET n = 3, v = 0bigdec, w = 99999999999999999999999999999999999999999999999999999999999999999bigdec;

SELECT * FROM pbd ORDER BY n;
SELECT VALUE [type::of(v), type::of(w)] FROM pbd ORDER BY n;
SELECT VALUE id FROM pbd WHERE v < 1 ORDER BY n;
SELECT VALUE id FROM pbd WHERE w > 1e60 ORDER BY n;

UPDATE pbd:a SET v = v * 2, w = 1.25bigdec;
SELECT v, w FROM pbd:a;

-- A value that is not a bigdecimal is refused by the field.
CREATE pbd:d SET n = 4, v = 'one';
