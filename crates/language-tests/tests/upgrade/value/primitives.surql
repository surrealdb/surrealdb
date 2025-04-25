/**
[env]
imports = ["upgrade/value/primitives_import.surql"]

[test]
upgrade = true
importing-version = ">=2.2.1"

[[test.results]]
value = '''[NONE, NULL, true, false, 10, 10f, 10dec, 1h, d'2012-01-01T08:00:00Z', u'00000000-0000-0000-0000-000000000000', [1, 2, 3], { a: 1 }, (1, 2), encoding::base64::decode("MjMyMw"), a:b, |a:1..100|, /a/, 0..10]'''

*/
SELECT VALUE data FROM ONLY test:values;
