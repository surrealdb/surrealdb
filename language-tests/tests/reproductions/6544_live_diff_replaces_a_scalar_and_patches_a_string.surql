/**
[test]
reason = "The operations a DIFF live query emits are the ones value::diff returns: a replaced scalar is a JSON-patch `replace`, and only a string becomes a `change` op carrying a text patch (issue 6544)"
issue = 6544

[[test.results]]
value = "[{ op: 'replace', path: '/a', value: 439 }]"

[[test.results]]
value = "[{ op: 'change', path: '/a', value: '@@ -1,1 +1,1 @@\n-x\n+y\n' }]"
*/
value::diff({ a: 301 }, { a: 439 });
value::diff({ a: 'x' }, { a: 'y' });
