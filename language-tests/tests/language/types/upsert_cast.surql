/**
[test]

[[test.results]]
error = "Could not cast into `int` using input `NONE`"

[[test.results]]
value = "[{ age: 18, id: person:test }]"

[[test.results]]
error = "Could not cast into `bool | int` using input `NONE`"

[[test.results]]
value = "[{ age: 18, enabled: true, id: person:test }]"

[[test.results]]
value = "[{ age: 18, enabled: true, id: person:test, name: 'Tobie Morgan Hitchcock' }]"

[[test.results]]
value = "[{ age: 18, enabled: true, id: person:test, name: 'Tobie Morgan Hitchcock', scores: {1f, 2f, 3f, 4f, 5f} }]"

[[test.results]]
value = "[{ age: 18, enabled: true, id: person:test, name: 'Tobie Morgan Hitchcock', scores: [1f, 1f, 2f, 2f, 3f, 3f, 4f, 4f, 5f, 5f] }]"

[[test.results]]
value = "[{ age: 18, enabled: true, id: person:test, name: 'Tobie Morgan Hitchcock', scores: {1f, 2f, 3f, 4f, 5f} }]"

[[test.results]]
error = "Expected `array<float,5>` but found a collection of length `10`"

*/
UPSERT person:test SET age = <int> NONE;
UPSERT person:test SET age = <int> '18';
UPSERT person:test SET enabled = <bool | int> NONE;
UPSERT person:test SET enabled = <bool | int> true;
UPSERT person:test SET name = <string> 'Tobie Morgan Hitchcock';
UPSERT person:test SET scores = <set<float>> [1,1,2,2,3,3,4,4,5,5];
UPSERT person:test SET scores = <array<float>> [1,1,2,2,3,3,4,4,5,5];
UPSERT person:test SET scores = <set<float, 5>> [1,1,2,2,3,3,4,4,5,5];
UPSERT person:test SET scores = <array<float, 5>> [1,1,2,2,3,3,4,4,5,5];
