/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_pairs.surql"]

[test]
reason = "A #>> path reads the value it starts from once, so a value that differs between evaluations is still one value: every row reaches the member of the object or the element of the array that the value is, and never NULL, whichever way the path steps before the last"

[[test.results]]
value = "[{ nulls: 0 }]"

[[test.results]]
value = "[{ nulls: 0 }]"

[[test.results]]
value = "[{ reached: 200 }]"

[[test.results]]
value = "[{ reached: 200 }]"

[[test.results]]
value = "[{ nulls: 200 }]"
*/

-- The comparison that picks the value is evaluated afresh each time the path reads it.
SELECT count(*) AS nulls FROM jpair WHERE (CASE WHEN random() < 0.5 THEN obj ELSE arr END) #>> '{-1}' IS NULL;

SELECT count(*) AS nulls FROM jpair WHERE (CASE WHEN random() < 0.5 THEN obj3 ELSE arr3 END) #>> '{-1,-1,-1}' IS NULL;

SELECT count(*) AS reached FROM jpair WHERE (CASE WHEN random() < 0.5 THEN obj ELSE arr END) #>> '{-1}' IN ('from-object', 'from-array');

SELECT count(*) AS reached FROM jpair WHERE (CASE WHEN random() < 0.5 THEN obj3 ELSE arr3 END) #>> '{-1,-1,-1}' IN ('from-object', 'from-array');

-- A step past the text that the path reached gives NULL for the object and the array alike.
SELECT count(*) AS nulls FROM jpair WHERE (CASE WHEN random() < 0.5 THEN obj3 ELSE arr3 END) #>> '{-1,-1,-1,-1}' IS NULL;
