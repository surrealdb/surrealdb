/**
[env]
namespace = true
database = true

[test]
reason = "A JSON patch path is an RFC 6901 pointer: ~1 and ~0 stand for / and ~ inside a token and . is an ordinary character, so a patch must reach the key it names and a diff must name a key in a form the patch reads back"
issue = 7516

[[test.results]]
value = '[{ a: { b: 3 }, "a.b": 2, "a/b": 1, id: t:1, "m~n": 8 }]'

[[test.results]]
value = '[{ a: { b: 3 }, "a.b": 97, "a/b": 99, id: t:1, "m~n": 98 }]'

[[test.results]]
value = "[{ op: 'replace', path: '/a.b', value: 3 }, { op: 'replace', path: '/a~1b', value: 2 }, { op: 'replace', path: '/m~0n', value: 9 }]"

[[test.results]]
value = '{ "a.b": 3, "a/b": 2, "m~n": 9 }'
*/

CREATE t:1 CONTENT { "a/b": 1, "m~n": 8, "a.b": 2, "a": { "b": 3 } };
UPDATE t:1 PATCH [
	{ op: 'replace', path: '/a~1b', value: 99 },
	{ op: 'replace', path: '/m~0n', value: 98 },
	{ op: 'replace', path: '/a.b', value: 97 }
] RETURN AFTER;
value::diff({ "a/b": 1, "m~n": 8, "a.b": 2 }, { "a/b": 2, "m~n": 9, "a.b": 3 });
value::patch({ "a/b": 1, "m~n": 8, "a.b": 2 }, value::diff({ "a/b": 1, "m~n": 8, "a.b": 2 }, { "a/b": 2, "m~n": 9, "a.b": 3 }));
