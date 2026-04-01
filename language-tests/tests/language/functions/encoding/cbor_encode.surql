/**
[test]
reason = "Test encoding::cbor::encode with various value types"

[[test.results]]
value = 'b"A163666F6F63626172"'

[[test.results]]
value = 'b"6568656C6C6F"'

[[test.results]]
value = 'b"60"'

[[test.results]]
value = 'b"182A"'

[[test.results]]
value = 'b"26"'

[[test.results]]
value = 'b"FB40091EB851EB851F"'

[[test.results]]
value = 'b"F5"'

[[test.results]]
value = 'b"F4"'

[[test.results]]
value = 'b"F6"'

[[test.results]]
value = 'b"C6F6"'

[[test.results]]
value = 'b"83010203"'

[[test.results]]
value = 'b"80"'

[[test.results]]
value = 'b"A0"'

[[test.results]]
value = 'b"A16475736572A2646E616D6565546F6269656474616773826561646D696E6475736572"'

[[test.results]]
value = 'b"82A162696401A162696402"'

*/

encoding::cbor::encode({ foo: "bar" });
encoding::cbor::encode("hello");
encoding::cbor::encode("");
encoding::cbor::encode(42);
encoding::cbor::encode(-7);
encoding::cbor::encode(3.14);
encoding::cbor::encode(true);
encoding::cbor::encode(false);
encoding::cbor::encode(NULL);
encoding::cbor::encode(NONE);
encoding::cbor::encode([1, 2, 3]);
encoding::cbor::encode([]);
encoding::cbor::encode({});
encoding::cbor::encode({ user: { name: "Tobie", tags: ["admin", "user"] } });
encoding::cbor::encode([{ id: 1 }, { id: 2 }]);
