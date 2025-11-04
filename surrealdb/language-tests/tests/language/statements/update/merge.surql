/**
[test]

[[test.results]]
value = "[{ age: 90, id: person:one, state: 'alive' }]"

[[test.results]]
value = "[{ age: 91, id: person:one, state: { deceased: '2025-01-07' } }]"

[[test.results]]
value = "[{ age: 91, id: person:one, state: { deceased: '2025-01-07', other: 'prop' } }]"

*/
INSERT INTO person {
    id: person:one,
    age: 90,
    state: "alive"
};

UPDATE person:one MERGE {
    age: 91,
    state: {
        deceased: "2025-01-07"
    }
};

UPDATE person:one MERGE {
    age: 91,
    state: {
        other: "prop"
    }
};