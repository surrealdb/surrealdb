/**
[test]
reason = "A bigdecimal in a record id addresses its record by value and scale, orders among the other ids of its table, and is read back from the key as written."

*/

CREATE rid:[1.5bigdec] SET n = 1;
CREATE rid:[123456789012345678901234567890123456789012345.6789bigdec] SET n = 2;
CREATE rid:[-0.00000000000000000000000000000000000000000001bigdec] SET n = 3;
CREATE rid:[2.50bigdec, 'x'] SET n = 4;
CREATE rid:{ v: 7bigdec } SET n = 5;

SELECT * FROM rid:[1.5bigdec];
SELECT * FROM rid:[123456789012345678901234567890123456789012345.6789bigdec];
SELECT * FROM rid:[2.50bigdec, 'x'];
SELECT * FROM rid:{ v: 7bigdec };

-- The id is read back from the key with its scale.
SELECT VALUE id FROM rid ORDER BY n;

-- Creating the same id again is refused.
CREATE rid:[1.5bigdec];

-- A range over the array ids.
SELECT VALUE n FROM rid:[0bigdec]..=[2bigdec];

-- The component read back from the key is a bigdecimal.
SELECT VALUE type::of(id[0]) FROM rid:[1.5bigdec];
