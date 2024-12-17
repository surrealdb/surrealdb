/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ data: { name: 'Tobie', test: true }, id: person:01JDSKKE3G1ERAJZSQN9BZV0CE }"
skip-record-id-key = true

[[test.results]]
value = "{ data: { name: 'Jaime', test: true }, id: person:01JDSKKE3P73CP1AD0XRF6WPXM }"
skip-record-id-key = true

*/
DEFINE FUNCTION fn::record::create($data: any) {
	RETURN CREATE ONLY person:ulid() CONTENT { data: $data } RETURN AFTER;
};
RETURN fn::record::create({ test: true, name: 'Tobie' });
RETURN fn::record::create({ test: true, name: 'Jaime' });
