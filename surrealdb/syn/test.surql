// A raw mathematical expression at the base of the query
3 * 5 = 15;

// A raw record expression at the base of the query
person:tobie->knows->?;

-- CREATE a new table record with a random id
CREATE person;

-- CREATE a new table record with a specific integer id
CREATE person:13059;

-- CREATE a new table record with a backtick escaped string
CREATE person:`some record id`;

-- CREATE a new table record with a angle bracket escaped string
CREATE person:⟨some record id⟩;

-- CREATE a new table record with an complex array based id
CREATE person:['London', time::now()];

-- CREATE a new table record with an complex object based id
CREATE person:{ city: 'London', time: time::now() };

-- CREATE a new table record with a randomly generated id
CREATE person:rand();

-- CREATE a new table record with a randomly generated ULID
CREATE person:ulid();

-- CREATE a new table record with a randomly generated UUID
CREATE person:uuid();

-- CREATE a new table record with a random id, and set the record content
CREATE person CONTENT { name: { first: 'Tobie', last: 'Morgan Hitchcock'} };

-- CREATE a new table record with a specific string id, and set the record content
CREATE person:tobie CONTENT { name: 'Tobie', siblings: [person:jaime] };

-- Define a new custom namespaced function for lowercasing a string
DEFINE FUNCTION fn::my::custom::namespaced::lowercase($name: string) {
	-- The following doesn't parse, as we don't yet support function chaining
    -- RETURN $name.lowercase();
	RETURN string::lowercase($name);
};

-- Use a scripting function in SELECT-statement field projections
SELECT *, function() { return this.a } AS b FROM [{ a: 1 }];

-- A really complex query
SELECT
    *,
	# This is a single line comment using hash
    tags[0].value,
	-- This is a single line comment using dashes
    tags[$].value,
	// This is a single line comment using slashes
    3s as duration,
	/*
		this is an multiline block comment, that
		can be used an interspersed within sql
	*/
    1.345 AS number,
	100000f AS float_number,
	100000dec AS decimal_number,
	<array<string, 5>> ["a", "b", "c", "d", "e"],
	-- The following doesn't parse, as we don't yet support function chaining
    -- tags[*].contains('database') AS has_tag,
    tags[*] CONTAINS 'database' AS `has-tag`,
    1749284739243842973049283492847029475.294874902847590284 AS decimal,
	->purchased->(phone, laptop WHERE new = true)<-purchased<-person.emails->sent->email.subject AS email_subjects,
	<-knows->person AS people_who_know_me,
    [
        test,
        <float> 13456,
        <decimal> 13456,
        {
            LET $people = (SELECT * FROM person);
			LET $animals = SELECT * FROM cat, dog;
            RETURN array::combine($people, $animals);
        },
        person:tobie->purchased->(phone, laptop WHERE new = true).sku,
        math::PI > 3.14,
        fn::my::custom::namespaced::function('Tobie'),
    ],
	math::PI > 3.14 AS check_constant,
    test AS `some thing`,
    d'2012-04-23T18:25:43.511Z' AS utctime,
    d'2012-04-23T18:25:43.511-08:00' AS pacifictime,
    {
        key: (3 + 1 + 2),
        other: 9 * 7,
        'some thing': {
            otherkey: 'text',
        }
    } AS object,
    rand::uuid::v4() AS a_uuid_field,
    u"ff36afd6-6689-4c02-8c8c-7df478924645" AS ⟨another id field⟩,
	if true {
		'Yay'
	} else {
		'Oops'
	} AS if_else
FROM
	$param,
	test,
	temp,
	test:thingy,
	|test:10|,
	|test:1..10|,
	user:4104810..,
	user:4104810..96830483,
	user:..96830483,
	temperature:['London', NONE]..=['London', time::now()]
WHERE IF true THEN 'YAY' ELSE 'OOPS' END
    AND (0.1341, 0.5719) INSIDE {
        type: 'Polygon',
        coordinates: [
            [[0.1341, 0.5719], [0.1341, 0.5719]]
        ]
    }
    AND (3 + 3 * 4) = 6
    AND 3 + 3 * 4 = 6
    AND ages CONTAINS 18
    AND `if` IS true
    AND 346 <= 789
    AND 678 >= 345
    AND (
        then = true
        OR `if` = true
        OR `create` is NONE
        OR `delete` = NULL
        OR `delete` INSIDE ['one', 'two', 'three']
    )
SPLIT test.things
VERSION d'2019-01-01T08:00:00Z'
TIMEOUT 2w;
