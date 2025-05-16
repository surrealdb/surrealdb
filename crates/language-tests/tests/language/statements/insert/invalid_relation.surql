/**
[test]

[[test.results]]
error = "Can not execute INSERT statement where property 'in' is: NONE"

[[test.results]]
error = "Can not execute INSERT statement where property 'id' is: NONE"

*/
INSERT RELATION INTO likes {
	id: 'object',
};

INSERT RELATION {
	in: person:1,
};
