/**
[test]
reason = "Ensure += and -= error on incompatible types instead of silently doing nothing, and support the same type combinations as + and -"

[[test.results]]
value = "[{ data: { money: 100 }, id: person:tom }]"

[[test.results]]
error = true

[[test.results]]
error = true

[[test.results]]
value = "[{ data: { answer: 42, money: 100 }, id: person:tom }]"

[[test.results]]
value = "[{ data: { answer: 42, money: 100, extra: true }, id: person:tom }]"

[[test.results]]
error = true

[[test.results]]
value = "[{ data: { answer: 42, money: 100, extra: true }, id: person:tom }]"
*/

CREATE person:tom SET data = { money: 100 };

-- Object += Number should error (previously silently did nothing)
UPDATE person:tom SET data += 10;

-- Object -= Number should error (previously silently did nothing)
UPDATE person:tom SET data -= 10;

-- Object += Object should merge (like + does for objects)
UPDATE person:tom SET data += { answer: 42 };

-- Verify multiple object merges work
UPDATE person:tom SET data += { extra: true };

-- Number += String should error
UPDATE person:tom SET data.money += "hello";

-- Verify the record is unchanged after the errors
SELECT * FROM person:tom;
