/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: user:1, name: 'Robert' }]"

[[test.results]]
value = "[{ id: city:london, name: 'London' }]"

[[test.results]]
value = "[{ id: city:u'8e60244d-95f6-4f95-9e30-09a98977efb0', name: 'London' }]"

[[test.results]]
value = "[{ id: temperature:['London', d'2022-09-30T20:25:01.406828Z'], name: 'London' }]"

[[test.results]]
value = "[{ id: test:715917898417176677 }]"

[[test.results]]
value = "[{ id: test:⟨715917898.417176677⟩ }]"

[[test.results]]
value = "[{ id: test:⟨9223372036854775808⟩ }]"

[[test.results]]
value = "[{ id: person:⟨8⟩, name: 'Tester' }]"

[[test.results]]
error = "Found '' for the Record ID but this is not a valid id"

[[test.results]]
error = "Found '' for the Record ID but this is not a valid id"

[[test.results]]
error = "Found 'tobie' for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Found 'tobie' for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Found person:1..2 for the Record ID but this is not a valid id"

*/

CREATE person:test SET name = 'Tester';
CREATE person SET id = person:tobie, name = 'Tobie';
CREATE person CONTENT { id: person:jaime, name: 'Jaime' };
CREATE user CONTENT { id: 1, name: 'Robert' };
CREATE city CONTENT { id: 'london', name: 'London' };
CREATE city CONTENT { id: u'8e60244d-95f6-4f95-9e30-09a98977efb0', name: 'London' };
CREATE temperature CONTENT { id: ['London', d'2022-09-30T20:25:01.406828Z'], name: 'London' };
CREATE test CONTENT { id: other:715917898417176677 };
CREATE test CONTENT { id: other:⟨715917898.417176677⟩ };
CREATE test CONTENT { id: other:9223372036854775808 };
CREATE person CONTENT { id: type::string(8), name: 'Tester' };
-- Should error as id is empty
CREATE person SET id = '';
CREATE person CONTENT { id: '', name: 'Tester' };
-- Should error as id is mismatched
CREATE person:other SET id = 'tobie';
CREATE person:other CONTENT { id: 'tobie', name: 'Tester' };
-- Should error as id is a range
CREATE person SET id = person:1..2;
