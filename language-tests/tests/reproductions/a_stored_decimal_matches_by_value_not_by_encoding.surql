/**
[env]
namespace = true
database = true

[test]
reason = """
A decimal is stored with its scale and with the sign of a zero, so `2.0dec` and
`2dec`, or `-0dec` and `0dec`, are one value held as different bytes. Every
comparison with a decimal literal matches the value, whichever way the record
or the literal spells it.

Decimal arithmetic keeps the scale of its operands (`1.10dec + 0.90dec` is
`2.0dec`), and negating a zero keeps the sign. The first `SELECT` reads back
the spelling each record holds, which is what makes every comparison below one
between two encodings of a value.

The negative zero is written `-<decimal> 0`: `-(0dec)` prints as `-0dec`, which
parses back as the literal `0dec`.

Each table scan judges a record's stored bytes with the pre-decode filter
before decoding it, which is the judgement under test.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[['2.0', '2.0'], ['-0', '-0'], ['4', '4'], ['0', '0']]"

[[test.results]]
value = "['4.0', true, true, true]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: [2.0dec] }]"

[[test.results]]
value = "[{ id: t:1, tags: 2.0dec }]"

[[test.results]]
value = "[{ id: t:2, tags: [-0dec] }, { id: t:4, tags: [0dec] }]"

[[test.results]]
value = "[{ id: t:2, tags: [-0dec] }, { id: t:4, tags: [0dec] }]"

[[test.results]]
value = "[{ id: t:2, tags: [-0dec] }, { id: t:4, tags: [0dec] }]"

[[test.results]]
value = "[{ id: t:3, tags: [4dec] }]"
*/

CREATE t:1 SET price = 1.10dec + 0.90dec, tags = [1.10dec + 0.90dec], qty = 1 RETURN NONE;
CREATE t:2 SET price = -<decimal> 0, tags = [-<decimal> 0], qty = 1 RETURN NONE;
CREATE t:3 SET price = 4dec, tags = [4dec], qty = 1 RETURN NONE;
CREATE t:4 SET price = 0dec, tags = [0dec], qty = 1 RETURN NONE;

SELECT VALUE [<string> price, <string> tags[0]] FROM t;
RETURN [<string> (1.10dec + 2.90dec), 1.10dec + 0.90dec = 2dec, -<decimal> 0 = 0dec, 1.10dec + 2.90dec = 4dec];

SELECT id, tags FROM t WHERE price = 2dec;
SELECT id, tags FROM t WHERE price == 2dec;
SELECT id, tags FROM t WHERE price = 2dec AND qty = 1;
SELECT id, tags FROM t WHERE !(price != 2dec);
SELECT id, tags FROM t WHERE price IN [2dec, 3dec];
SELECT id, tags FROM t WHERE price = 2dec OR price = 3dec;
SELECT id, tags FROM t WHERE tags CONTAINSANY [2dec];
SELECT id, tags FROM t WHERE tags CONTAINS 2dec;
SELECT id, tags FROM t WHERE price = 2dec SPLIT tags;

SELECT id, tags FROM t WHERE price = 0dec;
SELECT id, tags FROM t WHERE price IN [0dec];
SELECT id, tags FROM t WHERE tags CONTAINS 0dec;

SELECT id, tags FROM t WHERE price = 1.10dec + 2.90dec;
