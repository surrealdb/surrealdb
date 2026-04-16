/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
value = "[{ id: person:test, name: 'Name: Tobie' }]"

[[test.results]]
error = "Found 'Name: jaime' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"

[[test.results]]
value = "[{ id: person:test, name: 'Name: Jaime' }]"

[[test.results]]
error = "Found 'Name: tobie' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"

[[test.results]]
value = "[{ id: person:test, name: 'Name: Tobie' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Name: Tobie' }]"

*/
DEFINE FIELD name ON TABLE person
	ASSERT
		IF $input THEN
			$input = /^[A-Z]{1}[a-z]+$/
		ELSE
			true
		END
	VALUE
		IF $input THEN
			'Name: ' + $input
		ELSE
			$value
		END;

CREATE person:test;
UPDATE person:test CONTENT { name: 'Tobie' };
UPDATE person:test REPLACE { name: 'jaime' };
UPDATE person:test MERGE { name: 'Jaime' };
UPDATE person:test SET name = 'tobie';
UPDATE person:test SET name = 'Tobie';
SELECT * FROM person:test;
