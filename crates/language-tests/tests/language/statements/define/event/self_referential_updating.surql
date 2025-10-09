/**
[test]
reason = "Test self-referential updating with DEFINE EVENT and nested UPSERT operations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: city:london, id: person:tobie }]"

[[test.results]]
value = "[{ city: { citizens: [person:tobie], id: city:london }, id: person:tobie }]"

[[test.results]]
value = "[{ city: city:sanfrancisco, id: person:tobie }]"

[[test.results]]
value = "[{ city: { citizens: [person:tobie], id: city:sanfrancisco }, id: person:tobie }]"

[env]
namespace = "test"
database = "test"

*/

DEFINE EVENT updateCitizens ON TABLE person WHEN $before.city != $after.city THEN {
    IF $before.city {
        UPSERT $before.city SET citizens -= $value.id;
    };
    IF $after.city {
        UPSERT $after.city SET citizens += $value.id;
    };
};

UPSERT person:tobie SET city = city:london;

SELECT * FROM person FETCH city;

UPSERT person:tobie SET city = city:sanfrancisco;

SELECT * FROM person FETCH city;
