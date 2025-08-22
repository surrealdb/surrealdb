/**
[test]
wip = true


 [[test.results]]
 value = "NONE"

 [[test.results]]
 value = "NONE"

 [[test.results]]
 value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: { person_by_age: 'DEFINE TABLE person_by_age TYPE ANY SCHEMALESS AS SELECT count(), age, math::sum(age) AS total, math::mean(score) AS average, math::max(score) AS max, math::min(score) AS min FROM person GROUP BY age PERMISSIONS NONE' } }"

 [[test.results]]
 value = "[{ age: 39, id: person:one, score: 72 }]"

 [[test.results]]
 value = "[{ age: 39, average: 72dec, count: 1, id: person_by_age:[39], max: 72, min: 72, total: 39 }]"

 [[test.results]]
 value = "[{ age: 39, id: person:two, score: 83 }]"

 [[test.results]]
 value = "[{ age: 39, average: 77.50dec, count: 2, id: person_by_age:[39], max: 83, min: 72, total: 78 }]"

 [[test.results]]
 value = "[{ age: 39, id: person:two, score: 91 }]"

 [[test.results]]
 value = "[{ age: 39, average: 81.5dec, count: 2, id: person_by_age:[39], max: 91, min: 72, total: 78 }]"

[[test.results]]
error = "Incorrect arguments for aggregate function math::mean() on table 'person_by_age'. This function expects a number but found 'test'"

[[test.results]]
value = "[{ age: 39, average: 81.5dec, count: 2, id: person_by_age:[39], max: 91, min: 72, total: 78 }]"

*/
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE person_by_age AS
	SELECT
		count(),
		age,
		 math::sum(age) AS total,
		math::mean(score) AS average,
		 math::max(score) AS max,
		 math::min(score) AS min
	FROM person
	GROUP BY age
;
INFO FOR TABLE person;
UPSERT person:one SET age = 39, score = 72;
SELECT * FROM person_by_age;
UPSERT person:two SET age = 39, score = 83;
SELECT * FROM person_by_age;
UPSERT person:two SET age = 39, score = 91;
SELECT * FROM person_by_age;
UPSERT person:two SET age = 39, score = 'test';
SELECT * FROM person_by_age;
