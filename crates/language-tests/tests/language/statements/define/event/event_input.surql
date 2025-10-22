/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ double: 2, id: test:xs89n4c68mihcaxhu333 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ doubled: 2, id: log:9hq7cpm4e46n764esv85, input: 1 }]"
skip-record-id-key = true

*/

DEFINE FIELD double ON test TYPE number VALUE $value * 2;

DEFINE EVENT test ON TABLE test WHEN true THEN {
    CREATE log SET input = $input.double, doubled = $after.double;
};

CREATE test SET double = 1;
SELECT * FROM log;