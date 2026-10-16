/**
[env]
namespace = true
database = true

[test]
reason = "Nested objects of a SCHEMAFULL table, typed and open, are read through their enclosing nodes: a field of a present node, an absent node, a NULL node and the keys an open node holds beyond its columns."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ extra: { z: 1 }, flex: { extra: 1, other: [1, 2] }, geography: { code: 'JP', region: { details: { continent: 'Asia', timezone: 'JST' }, name: 'Kanto' }, text: 'Japan' }, id: place:1, maybe: { k: 1 }, name: 'tokyo' }]"

[[test.results]]
value = "[{ flex: {  }, geography: { code: 'FR', region: { details: { continent: 'Europe' }, name: 'IDF' } }, id: place:2, name: 'paris' }]"

[[test.results]]
value = "[{ flex: { deep: { a: { b: { c: 3 } } }, extra: 2 }, geography: { code: 'PE', text: 'Peru' }, id: place:3, maybe: {  }, name: 'lima' }]"

[[test.results]]
value = "[{ flex: { extra: 1 }, geography: { code: 'EG', region: { details: { continent: 'Africa', timezone: 'EET' }, name: 'Cairo' } }, id: place:4, maybe: { k: 4 }, name: 'cairo' }]"

[[test.results]]
value = "[{ flex: { extra: NULL }, geography: { code: 'JP', region: { details: { continent: 'Asia' }, name: 'Kansai' }, text: '' }, id: place:5, name: 'osaka' }]"

[[test.results]]
value = "[{ extra: { z: 1 }, flex: { extra: 1, other: [1, 2] }, geography: { code: 'JP', region: { details: { continent: 'Asia', timezone: 'JST' }, name: 'Kanto' }, text: 'Japan' }, id: place:1, maybe: { k: 1 }, name: 'tokyo' }, { flex: {  }, geography: { code: 'FR', region: { details: { continent: 'Europe' }, name: 'IDF' } }, id: place:2, name: 'paris' }, { flex: { deep: { a: { b: { c: 3 } } }, extra: 2 }, geography: { code: 'PE', text: 'Peru' }, id: place:3, maybe: {  }, name: 'lima' }, { flex: { extra: 1 }, geography: { code: 'EG', region: { details: { continent: 'Africa', timezone: 'EET' }, name: 'Cairo' } }, id: place:4, maybe: { k: 4 }, name: 'cairo' }, { flex: { extra: NULL }, geography: { code: 'JP', region: { details: { continent: 'Asia' }, name: 'Kansai' }, text: '' }, id: place:5, name: 'osaka' }]"

[[test.results]]
value = "[place:1, place:5]"

[[test.results]]
value = "[place:2, place:3, place:5]"

[[test.results]]
value = "[place:1]"

[[test.results]]
value = "[place:1]"

[[test.results]]
value = "[place:3]"

[[test.results]]
value = "[place:1, place:2, place:4, place:5]"

[[test.results]]
value = "[place:5]"

[[test.results]]
value = "[place:2, place:4]"

[[test.results]]
value = "[place:1, place:5]"

[[test.results]]
value = "[place:2]"

[[test.results]]
value = "[{ geography: { code: 'JP', region: { details: { continent: 'Asia', timezone: 'JST' }, name: 'Kanto' }, text: 'Japan' } }]"

[[test.results]]
value = "[{ geography: { region: { details: { continent: 'Asia', timezone: 'JST' }, name: 'Kanto' } } }, { geography: { region: { details: { continent: 'Europe' }, name: 'IDF' } } }, { geography: { region: NONE } }, { geography: { region: { details: { continent: 'Africa', timezone: 'EET' }, name: 'Cairo' } } }, { geography: { region: { details: { continent: 'Asia' }, name: 'Kansai' } } }]"

[[test.results]]
value = "[{ geography: { region: { details: { continent: 'Asia', timezone: 'JST' } } } }, { geography: { region: { details: { continent: 'Europe' } } } }, { geography: { region: { details: NONE } } }, { geography: { region: { details: { continent: 'Africa', timezone: 'EET' } } } }, { geography: { region: { details: { continent: 'Asia' } } } }]"

[[test.results]]
value = "['Asia', 'Europe', NONE, 'Africa', 'Asia']"

[[test.results]]
value = "[{ k: 1 }, NONE, {  }, { k: 4 }, NONE]"

[[test.results]]
value = "[place:2, place:5]"

[[test.results]]
value = "[place:1]"

[[test.results]]
value = "[place:2, place:3, place:5]"

[[test.results]]
value = "[place:3]"

[[test.results]]
value = "[{ extra: 1, other: [1, 2] }, {  }, { deep: { a: { b: { c: 3 } } }, extra: 2 }, { extra: 1 }, { extra: NULL }]"

[[test.results]]
value = "[place:1, place:4]"

[[test.results]]
value = "[place:5]"

[[test.results]]
value = "[place:2]"

[[test.results]]
value = "[place:3]"

[[test.results]]
value = "[place:1]"

[[test.results]]
value = "[[1, 2], NONE, NONE, NONE, NONE]"

[[test.results]]
value = "[{ z: 1 }, NONE, NONE, NONE, NONE]"

[[test.results]]
value = "[place:1]"

[[test.results]]
value = "[place:2, place:3, place:4, place:5]"

[[test.results]]
value = "[{ flex: { extra: 1 }, geography: { code: 'EG', region: { details: { continent: 'Africa', timezone: 'EET' }, name: 'Cairo' } }, id: place:4, maybe: { k: 4 }, name: 'cairo' }]"

[[test.results]]
value = "[{ geography: { code: 'FR' }, name: 'paris' }, { geography: { code: 'PE' }, name: 'lima' }, { geography: { code: 'EG' }, name: 'cairo' }]"

[[test.results]]
value = "[{ c: 1, code: 'EG' }, { c: 1, code: 'FR' }, { c: 2, code: 'JP' }, { c: 1, code: 'PE' }]"

[[test.results]]
value = "[{ count: 2 }]"
*/

DEFINE TABLE place SCHEMAFULL;
DEFINE FIELD name ON place TYPE string;
DEFINE FIELD geography ON place TYPE object;
DEFINE FIELD geography.code ON place TYPE string;
DEFINE FIELD geography.text ON place TYPE option<string>;
DEFINE FIELD geography.region ON place TYPE option<object>;
DEFINE FIELD geography.region.name ON place TYPE string;
DEFINE FIELD geography.region.details ON place TYPE object;
DEFINE FIELD geography.region.details.continent ON place TYPE string;
DEFINE FIELD geography.region.details.timezone ON place TYPE option<string>;
DEFINE FIELD maybe ON place TYPE option<object>;
DEFINE FIELD maybe.k ON place TYPE option<int>;
DEFINE FIELD flex ON place TYPE object FLEXIBLE;
DEFINE FIELD extra ON place TYPE option<object> FLEXIBLE;
CREATE place:1 CONTENT { name: 'tokyo', geography: { code: 'JP', text: 'Japan', region: { name: 'Kanto', details: { continent: 'Asia', timezone: 'JST' } } }, maybe: { k: 1 }, flex: { extra: 1, other: [1, 2] }, extra: { z: 1 } };
CREATE place:2 CONTENT { name: 'paris', geography: { code: 'FR', region: { name: 'IDF', details: { continent: 'Europe' } } },  flex: {} };
CREATE place:3 CONTENT { name: 'lima', geography: { code: 'PE', text: 'Peru' }, maybe: { }, flex: { extra: 2, deep: { a: { b: { c: 3 } } } } };
CREATE place:4 CONTENT { name: 'cairo', geography: { code: 'EG', region: { name: 'Cairo', details: { continent: 'Africa', timezone: 'EET' } } }, maybe: { k: 4 }, flex: { extra: 1 } };
CREATE place:5 CONTENT { name: 'osaka', geography: { code: 'JP', text: '', region: { name: 'Kansai', details: { continent: 'Asia' } } },  flex: { extra: NULL } };

SELECT * FROM place;
SELECT VALUE id FROM place WHERE geography.region.details.continent = 'Asia';
SELECT VALUE id FROM place WHERE geography.region.details.timezone IS NONE;
SELECT VALUE id FROM place WHERE geography.region.details.timezone = 'JST';
SELECT VALUE id FROM place WHERE geography.region.name = 'Kanto';
SELECT VALUE id FROM place WHERE geography.region IS NONE;
SELECT VALUE id FROM place WHERE geography.region IS NOT NONE;
SELECT VALUE id FROM place WHERE geography.text = '';
SELECT VALUE id FROM place WHERE geography.text IS NONE;
SELECT VALUE id FROM place WHERE geography.code = 'JP';
SELECT VALUE id FROM place WHERE geography = { code: 'FR', region: { name: 'IDF', details: { continent: 'Europe' } } };
SELECT geography FROM place WHERE name = 'tokyo';
SELECT geography.region FROM place;
SELECT geography.region.details FROM place;
SELECT VALUE geography.region.details.continent FROM place;
SELECT VALUE maybe FROM place;
SELECT VALUE id FROM place WHERE maybe IS NONE;
SELECT VALUE id FROM place WHERE maybe.k = 1;
SELECT VALUE id FROM place WHERE maybe.k IS NONE;
SELECT VALUE id FROM place WHERE maybe = {};
SELECT VALUE flex FROM place;
SELECT VALUE id FROM place WHERE flex.extra = 1;
SELECT VALUE id FROM place WHERE flex.extra IS NULL;
SELECT VALUE id FROM place WHERE flex.extra IS NONE;
SELECT VALUE id FROM place WHERE flex.deep.a.b.c = 3;
SELECT VALUE id FROM place WHERE flex.other[1] = 2;
SELECT VALUE flex.other FROM place;
SELECT VALUE extra FROM place;
SELECT VALUE id FROM place WHERE extra.z = 1;
SELECT VALUE id FROM place WHERE extra IS NONE;
SELECT * FROM place WHERE geography.region.details.continent = 'Africa';
SELECT name, geography.code FROM place WHERE geography.region.details.continent != 'Asia';
SELECT geography.code AS code, count() AS c FROM place GROUP BY geography.code ORDER BY code;
SELECT count() FROM place WHERE geography.region.details.continent = 'Asia' GROUP ALL;
