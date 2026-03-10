/**
[test]
reason = "Test recursive record link traversal with various modes (path, collect, destructure)"

[[test.results]]
value = "[{ contains: [country:us, country:canada], id: planet:earth, name: 'Earth' }, { contains: [state:california, state:texas], id: country:us, name: 'United States' }, { contains: [province:ontario, province:bc], id: country:canada, name: 'Canada' }, { contains: [city:los_angeles, city:san_francisco], id: state:california, name: 'California' }, { contains: [city:houston, city:dallas], id: state:texas, name: 'Texas' }, { contains: [city:toronto, city:ottawa], id: province:ontario, name: 'Ontario' }, { contains: [city:vancouver, city:victoria], id: province:bc, name: 'British Columbia' }, { id: city:los_angeles, name: 'Los Angeles' }, { id: city:san_francisco, name: 'San Francisco' }, { id: city:houston, name: 'Houston' }, { id: city:dallas, name: 'Dallas' }, { id: city:toronto, name: 'Toronto' }, { id: city:ottawa, name: 'Ottowa' }, { id: city:vancouver, name: 'Vancouver' }, { id: city:victoria, name: 'Victoria' }]"

[[test.results]]
value = "['United States', 'Canada']"

[[test.results]]
value = "['California', 'Texas', 'Ontario', 'British Columbia']"

[[test.results]]
value = "['Los Angeles', 'San Francisco', 'Houston', 'Dallas', 'Toronto', 'Ottowa', 'Vancouver', 'Victoria']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[country:us, state:california], [country:us, state:texas], [country:canada, province:ontario], [country:canada, province:bc]]"

[[test.results]]
value = "[['United States', 'California'], ['United States', 'Texas'], ['Canada', 'Ontario'], ['Canada', 'British Columbia']]"

[[test.results]]
value = "[[planet:earth, country:us, state:california], [planet:earth, country:us, state:texas], [planet:earth, country:canada, province:ontario], [planet:earth, country:canada, province:bc]]"

[[test.results]]
value = "[state:california, state:texas, province:ontario, province:bc]"

[[test.results]]
value = "['California', 'Texas', 'Ontario', 'British Columbia']"

[[test.results]]
value = "[planet:earth, state:california, state:texas, province:ontario, province:bc]"

[[test.results]]
value = "[country:us, country:canada]"

[[test.results]]
value = "[state:california, state:texas, province:ontario, province:bc]"

[[test.results]]
value = "[city:los_angeles, city:san_francisco, city:houston, city:dallas, city:toronto, city:ottawa, city:vancouver, city:victoria]"

[[test.results]]
value = "['United States', 'Canada']"

[[test.results]]
value = "['California', 'Texas', 'Ontario', 'British Columbia']"

[[test.results]]
value = "['Los Angeles', 'San Francisco', 'Houston', 'Dallas', 'Toronto', 'Ottowa', 'Vancouver', 'Victoria']"

[[test.results]]
value = "{ id: planet:earth, name: 'Earth', places: [country:us, country:canada] }"

[[test.results]]
value = "{ id: planet:earth, name: 'Earth', places: [{ id: country:us, name: 'United States', places: [state:california, state:texas] }, { id: country:canada, name: 'Canada', places: [province:ontario, province:bc] }] }"

[[test.results]]
value = "{ id: planet:earth, name: 'Earth', places: [{ id: country:us, name: 'United States', places: [{ id: state:california, name: 'California', places: [city:los_angeles, city:san_francisco] }, { id: state:texas, name: 'Texas', places: [city:houston, city:dallas] }] }, { id: country:canada, name: 'Canada', places: [{ id: province:ontario, name: 'Ontario', places: [city:toronto, city:ottawa] }, { id: province:bc, name: 'British Columbia', places: [city:vancouver, city:victoria] }] }] }"

[[test.results]]
value = "{ id: planet:earth, name: 'Earth', places: [{ id: country:us, name: 'United States', places: [{ id: state:california, name: 'California', places: [{ id: city:los_angeles, name: 'Los Angeles', places: NONE }, { id: city:san_francisco, name: 'San Francisco', places: NONE }] }, { id: state:texas, name: 'Texas', places: [{ id: city:houston, name: 'Houston', places: NONE }, { id: city:dallas, name: 'Dallas', places: NONE }] }] }, { id: country:canada, name: 'Canada', places: [{ id: province:ontario, name: 'Ontario', places: [{ id: city:toronto, name: 'Toronto', places: NONE }, { id: city:ottawa, name: 'Ottowa', places: NONE }] }, { id: province:bc, name: 'British Columbia', places: [{ id: city:vancouver, name: 'Vancouver', places: NONE }, { id: city:victoria, name: 'Victoria', places: NONE }] }] }] }"

[[test.results]]
value = "{ id: planet:earth, name: 'Earth', places: [{ id: country:us, name: 'United States', places: [{ id: state:california, name: 'California', places: [{ id: city:los_angeles, name: 'Los Angeles', places: NONE }, { id: city:san_francisco, name: 'San Francisco', places: NONE }] }, { id: state:texas, name: 'Texas', places: [{ id: city:houston, name: 'Houston', places: NONE }, { id: city:dallas, name: 'Dallas', places: NONE }] }] }, { id: country:canada, name: 'Canada', places: [{ id: province:ontario, name: 'Ontario', places: [{ id: city:toronto, name: 'Toronto', places: NONE }, { id: city:ottawa, name: 'Ottowa', places: NONE }] }, { id: province:bc, name: 'British Columbia', places: [{ id: city:vancouver, name: 'Vancouver', places: NONE }, { id: city:victoria, name: 'Victoria', places: NONE }] }] }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: planet:earth, name: 'Earth', places: [{ id: country:us, name: 'United States', places: [{ id: state:california, name: 'California', places: [{ id: city:los_angeles, name: 'Los Angeles', places: [] }, { id: city:san_francisco, name: 'San Francisco', places: [] }] }, { id: state:texas, name: 'Texas', places: [{ id: city:houston, name: 'Houston', places: [] }, { id: city:dallas, name: 'Dallas', places: [] }] }] }, { id: country:canada, name: 'Canada', places: [{ id: province:ontario, name: 'Ontario', places: [{ id: city:toronto, name: 'Toronto', places: [] }, { id: city:ottawa, name: 'Ottowa', places: [] }] }, { id: province:bc, name: 'British Columbia', places: [{ id: city:vancouver, name: 'Vancouver', places: [] }, { id: city:victoria, name: 'Victoria', places: [] }] }] }] }"

[env]
timeout = 10000
*/

INSERT [
	{ id: planet:earth, name: 'Earth', contains: [country:us, country:canada] },
	{ id: country:us, name: 'United States', contains: [state:california, state:texas] },
	{ id: country:canada, name: 'Canada', contains: [province:ontario, province:bc] },
	{ id: state:california, name: 'California', contains: [city:los_angeles, city:san_francisco] },
	{ id: state:texas, name: 'Texas', contains: [city:houston, city:dallas] },
	{ id: province:ontario, name: 'Ontario', contains: [city:toronto, city:ottawa] },
	{ id: province:bc, name: 'British Columbia', contains: [city:vancouver, city:victoria] },
	{ id: city:los_angeles, name: 'Los Angeles' },
	{ id: city:san_francisco, name: 'San Francisco' },
	{ id: city:houston, name: 'Houston' },
	{ id: city:dallas, name: 'Dallas' },
	{ id: city:toronto, name: 'Toronto' },
	{ id: city:ottawa, name: 'Ottowa' },
	{ id: city:vancouver, name: 'Vancouver' },
	{ id: city:victoria, name: 'Victoria' },
];

planet:earth.{1}(.contains).name;
planet:earth.{2}(.contains).name;
planet:earth.{3}(.contains).name;
planet:earth.{4}(.contains).name;

planet:earth.{2+path}(.contains);
planet:earth.{2+path}(.contains).name;
planet:earth.{2+path+inclusive}(.contains);

planet:earth.{2+collect}(.contains);
planet:earth.{2+collect}(.contains).name;
planet:earth.{2+collect+inclusive}(.contains);

planet:earth.{1}.contains.@;
planet:earth.{2}.contains.@;
planet:earth.{3}.contains.@;
planet:earth.{1}.contains.@.name;
planet:earth.{2}.contains.@.name;
planet:earth.{3}.contains.@.name;

planet:earth.{1}.{ id, name, places: contains.@ };
planet:earth.{2}.{ id, name, places: contains.@ };
planet:earth.{3}.{ id, name, places: contains.@ };
planet:earth.{4}.{ id, name, places: contains.@ };
planet:earth.{..}.{ id, name, places: contains.@ };
planet:earth.{5..}.{ id, name, places: contains.@ };

planet:earth.{..}.{ id, name, places: contains.@.chain(|$v| $v ?? []) };
